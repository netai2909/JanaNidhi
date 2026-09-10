"""
MPLADS data ingestion pipeline — live portal only.

The single data source is the live MPLADS dashboard API
(mplads.mospi.gov.in /digigov internal REST endpoint, implemented in
backend/services/mplads_live.py). Every sync fetches fresh per-work
records for the houses selected via MPLADS_LIVE_HOUSE, scores them with
the risk engine, and upserts. `mode="auto"` is an alias for "live".

An explicit `source_file_path` may be passed for tests / offline replays;
it follows the same long-format contract and bypasses the network.

Every run appends exactly one entry to sync_logs. On failure the
last-known-good database is preserved and the failure is logged.
"""
import time
from datetime import datetime, timezone
from pathlib import Path
import logging

import pandas as pd
from pymongo import ReplaceOne, UpdateOne

from backend.database import works, mp_allocations, sync_logs, next_id
from backend.models import now_utc, lower_or_none
from backend.config import settings
from model.risk_engine import score_dataset

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Canonical source label used across the sync log UI
SOURCE_LIVE = "MPLADS Live Dashboard API (mplads.mospi.gov.in)"

VALID_MODES = {"auto", "live"}

_UPSERT_CHUNK = 1000  # bulk_write operations per round-trip



# ------------------------------------------------------------------------------
# Normalization & scoring (shared with file-based offline replays)
# ------------------------------------------------------------------------------

def _validate_house(df: pd.DataFrame) -> None:
    """Log warnings for missing or unknown house values in the dataframe.
    The ingestion pipeline expects a 'house' column indicating Lok Sabha or Rajya Sabha.
    """
    if "house" not in df.columns:
        logger.warning("Dataframe missing 'house' column — house will not be tracked.")
        return
    missing = df["house"].isna() | (df["house"].astype(str).str.strip() == "")
    if missing.any():
        logger.warning("%d rows have missing house information.", missing.sum())


def _reshape_long_format(df: pd.DataFrame) -> pd.DataFrame:
    """Convert the portal's long format (record_type rows) into work-level facts.

    Validates and preserves the 'house' column (Lok Sabha / Rajya Sabha)
    throughout the reshape so downstream consumers receive it correctly.
    """
    _validate_house(df)

    if "record_type" not in df.columns:
        if "total_fund_disbursed" not in df.columns:
            df["total_fund_disbursed"] = 0.0
        return df

    sanctioned = df[df["record_type"] == "Works Sanctioned"].copy()
    if len(sanctioned) == 0:
        sanctioned = df.dropna(subset=["work_id"]).drop_duplicates("work_id").copy()
    else:
        sanctioned = sanctioned.drop_duplicates("work_id")

    completed = df[df["record_type"] == "Works Completed"].copy()
    expenditure = df[df["record_type"] == "Expenditure on Completed & On-going Works"].copy()

    if len(expenditure) > 0 and "fund_disbursed_amount" in expenditure.columns:
        expenditure["fund_disbursed_amount"] = pd.to_numeric(
            expenditure["fund_disbursed_amount"], errors="coerce"
        ).fillna(0)
        # Rich per-work payment aggregates power the risk engine's vendor,
        # stall and post-completion billing signals.
        exp_agg = expenditure.groupby("work_id").agg(
            total_fund_disbursed=("fund_disbursed_amount", "sum"),
            n_vendor_payments=("fund_disbursed_amount", "count"),
            primary_vendor=("vendor_name", lambda s: s.dropna().mode().iat[0] if not s.dropna().mode().empty else None),
            last_expenditure_date=("expenditure_date", lambda s: s.dropna().max() if s.notna().any() else None),
            payment_statuses=("payment_status", lambda s: "|".join(sorted({str(x) for x in s.dropna()}))),
        ).reset_index()
        exp_agg["n_distinct_vendors"] = expenditure.dropna(subset=["vendor_name"]).groupby("work_id")["vendor_name"].nunique()
        sanctioned = sanctioned.merge(exp_agg, on="work_id", how="left")

    if len(completed) > 0 and "amount_disbursed" in completed.columns:
        comp_slim = completed[["work_id", "completion_date", "amount_disbursed"]].dropna(
            subset=["work_id"]
        ).drop_duplicates("work_id")
        sanctioned = sanctioned.merge(comp_slim, on="work_id", how="left", suffixes=("", "_comp"))
        # Sanctioned rows carry empty completion_date/amount_disbursed columns, so
        # the merged values land in *_comp — coalesce them back into the real ones.
        for col in ("completion_date", "amount_disbursed"):
            comp_col = f"{col}_comp"
            if comp_col in sanctioned.columns:
                sanctioned[col] = sanctioned[col].where(sanctioned[col].notna(), sanctioned[comp_col])
                sanctioned.drop(columns=[comp_col], inplace=True)

    if "total_fund_disbursed" not in sanctioned.columns:
        sanctioned["total_fund_disbursed"] = 0.0
    sanctioned["total_fund_disbursed"] = sanctioned["total_fund_disbursed"].fillna(0.0)

    # Preserve the 'house' column from the source dataframe.
    # Use index alignment so house labels survive any merge reindexing.
    if "house" in df.columns and "house" not in sanctioned.columns:
        house_map = df.drop_duplicates("work_id").set_index("work_id")["house"]
        if "work_id" in sanctioned.columns:
            sanctioned["house"] = sanctioned["work_id"].map(house_map)
        else:
            sanctioned["house"] = df.loc[sanctioned.index, "house"].values

    return sanctioned



def _normalize_work(row: pd.Series) -> dict:
    """Map any scored row into a works document (used for bulk upserts)."""
    def _s(key, default=None):
        val = row.get(key)
        return None if pd.isna(val) else str(val)

    def _f(key, default=0.0):
        val = row.get(key, default)
        try:
            return float(default if pd.isna(val) else val)
        except (TypeError, ValueError):
            return float(default)

    flags = row.get("rule_flags_triggered", "[]")
    if isinstance(flags, (list, tuple)):
        flags = list(flags)
    elif isinstance(flags, str):
        # CSV round-trips serialize lists back to strings — parse them so the
        # document stores a real array like every other consumer expects.
        parsed = _parse_flags_string(flags)
        flags = parsed

    # House must be propagated by the ingestion pipeline via the 'house' column.
    # We no longer infer it from constituency to avoid silently misclassifying MPs.
    house = _s("house")
    if not house:
        logger.warning(
            "Row work_id=%s is missing 'house' value; defaulting to 'Lok Sabha'. "
            "Check the fetch/reshape pipeline.",
            row.get("work_id"),
        )
        house = "Lok Sabha"

    # CSV round-trips can turn numeric work IDs into '1000.0' — strip it
    work_id = str(row["work_id"])
    if work_id.endswith(".0"):
        work_id = work_id[:-2]

    mp_name = _s("mp_name")
    state = _s("state")
    return {
        "work_id": work_id,
        "mp_name": mp_name,
        "_mp_name_lower": lower_or_none(mp_name),
        "state": state,
        "_state_lower": lower_or_none(state),
        "constituency": _s("constituency"),
        "house": house,
        "ida": _s("ida"),
        "primary_vendor": _s("primary_vendor"),
        "work_category": _s("work_category"),
        "work_type": _s("work_type"),
        "sanction_amount": _f("sanction_amount"),
        "total_fund_disbursed": _f("total_fund_disbursed"),
        "utilization_ratio": _f("utilization_ratio"),
        "work_status": _s("work_status"),
        "completion_date": _s("completion_date"),
        "final_risk_score": _f("final_risk_score"),
        "priority_rank": int(_f("priority_rank", 999999)),
        "risk_tier": _s("risk_tier") or "Low Risk",
        "recommended_action": _s("recommended_action") or "Routine monitoring",
        "rule_flag_count": int(_f("rule_flag_count")),
        "rule_flags_triggered": flags,
        "likelihood_score": _f("likelihood_score"),
        "impact_score": _f("impact_score"),
        "weighted_rule_score": _f("weighted_rule_score"),
        "anomaly_percentile": _f("anomaly_percentile"),
        "is_anomaly": bool(row.get("is_anomaly", False)),
    }


def _parse_flags_string(raw: str) -> list:
    """Parse a serialized flags string ('['a', 'b']') back into a list."""
    import ast
    try:
        parsed = ast.literal_eval(raw)
        return [str(f) for f in parsed] if isinstance(parsed, (list, tuple)) else []
    except (ValueError, SyntaxError):
        return [f.strip() for f in raw.strip("[]").replace("'", "").split(",") if f.strip()]


def _upsert_dataframe(df: pd.DataFrame) -> dict:
    """Chunked bulk upsert — one round-trip per ~1000 rows, not per row.
    Each chunk commits immediately so concurrent API reads stay live during
    long syncs."""
    inserted = updated = 0
    now = now_utc()
    rows = [r for _, r in df.iterrows() if not pd.isna(r.get("work_id"))]

    for start in range(0, len(rows), _UPSERT_CHUNK):
        chunk = rows[start:start + _UPSERT_CHUNK]
        mappings = [_normalize_work(r) for r in chunk]
        ids = [m["work_id"] for m in mappings]

        existing_ids = set(works.distinct("work_id", {"work_id": {"$in": ids}}))

        ops = []
        for m in mappings:
            m["updated_at"] = now
            if m["work_id"] in existing_ids:
                ops.append(UpdateOne({"work_id": m["work_id"]}, {"$set": m}))
                updated += 1
            else:
                m["created_at"] = now
                ops.append(ReplaceOne({"work_id": m["work_id"]}, m, upsert=True))
                inserted += 1
        if ops:
            works.bulk_write(ops, ordered=False)

    return {"inserted": inserted, "updated": updated, "processed": len(rows)}


def _upsert_allocations(long_df: pd.DataFrame) -> int:
    """Upsert the per-MP allocated funds from the portal's Allocated Limit
    dataset into mp_allocations, keyed on (mp_name, house, constituency, state)."""
    alloc = long_df[long_df.get("record_type") == "MP Allocated Limit"]
    if alloc.empty:
        return 0
    now = now_utc()
    ops = []
    count = 0
    for _, r in alloc.iterrows():
        if pd.isna(r.get("mp_name")):
            continue
        raw_house = r.get("house")
        if raw_house and not pd.isna(raw_house):
            house_val = str(raw_house)
        else:
            logger.warning(
                "Allocation row for mp_name=%s missing 'house'; defaulting to 'Lok Sabha'. "
                "Check the fetch pipeline.",
                r.get("mp_name"),
            )
            house_val = "Lok Sabha"

        mp_name = str(r["mp_name"])
        key = dict(
            mp_name=mp_name,
            house=house_val,
            constituency=str(r.get("constituency") or ""),
            state=str(r.get("state") or ""),
        )
        values = dict(
            allocated_amount=float(r.get("allocated_amount") or 0),
            tenure_start=str(r.get("recommended_date")) if pd.notna(r.get("recommended_date")) else None,
            updated_at=now,
        )
        doc = {**key, **values, "_mp_name_lower": lower_or_none(mp_name)}
        ops.append(ReplaceOne(key, doc, upsert=True))
        count += 1
    if ops:
        mp_allocations.bulk_write(ops, ordered=False)
    return count


def _log_sync(*, source, status, start_dt, counts=None, note=None):
    end_dt = now_utc()
    counts = counts or {}
    sync_logs.insert_one({
        "id": next_id("sync_logs"),
        "run_timestamp": start_dt,
        "start_time": start_dt,
        "end_time": end_dt,
        "status": status,
        "source": source,
        "rows_fetched": counts.get("fetched", 0),
        "rows_processed": counts.get("processed", 0),
        "rows_inserted": counts.get("inserted", 0),
        "rows_updated": counts.get("updated", 0),
        "rows_rejected": counts.get("rejected", 0),
        "error_message": note,
    })


# ------------------------------------------------------------------------------
# Public entry points
# ------------------------------------------------------------------------------

def run_ingestion(mode: str = "auto", source_file_path: Path = None) -> dict:
    """
    Run the ingestion pipeline.

    Modes: "auto" / "live" — fetch the live MPLADS dashboard API, risk-score,
    and upsert. Each run writes exactly one sync_logs entry.
    """
    if mode not in VALID_MODES:
        raise ValueError(f"Invalid ingestion mode '{mode}'. Must be one of {sorted(VALID_MODES)}")

    start_dt = now_utc()
    t0 = time.time()

    try:
        if source_file_path is not None:
            # Explicit file override (tests / offline replays)
            df = pd.read_csv(source_file_path)
            scored = score_dataset(_reshape_long_format(df), model_dir=settings.MODEL_DIR)
            counts = _upsert_dataframe(scored)
            counts["allocations"] = _upsert_allocations(df)
            counts["fetched"] = len(df)
            label = f"Ingestion Feed (file: {Path(source_file_path).name})"
            _log_sync(source=label, status="success", start_dt=start_dt, counts=counts)
            return {"status": "success", "mode": "file", "source": label,
                    "duration_seconds": round(time.time() - t0, 2), **counts}

        # ---- Live MPLADS dashboard API ------------------------------------
        try:
            import gc
            from backend.services.mplads_live import fetch_live_long_dataframe
            raw = fetch_live_long_dataframe(houses=settings.MPLADS_LIVE_HOUSE)

            # Cache raw feed selectively
            try:
                cache_path = settings.DATA_DIR / "last_live_feed.csv"
                raw.to_csv(cache_path, index=False)
            except Exception:
                pass

            alloc_map = {
                a["mp_name"]: a.get("allocated_amount") or 0.0
                for a in mp_allocations.find({}, {"mp_name": 1, "allocated_amount": 1})
            }

            reshaped = _reshape_long_format(raw)
            scored = score_dataset(reshaped, model_dir=settings.MODEL_DIR, mp_allocations=alloc_map)
            del reshaped
            gc.collect()

            counts = _upsert_dataframe(scored)
            counts["allocations"] = _upsert_allocations(raw)
            counts["fetched"] = len(raw)

            del raw, scored
            gc.collect()

            _log_sync(source=SOURCE_LIVE, status="success",
                      start_dt=start_dt, counts=counts)
            return {"status": "success", "mode": "live", "source": SOURCE_LIVE,
                    "duration_seconds": round(time.time() - t0, 2), **counts}
        except Exception as e:
            note = f"Live dashboard API unavailable ({e})"
            _log_sync(source=SOURCE_LIVE, status="failed",
                      start_dt=start_dt, note=note)
            return {"status": "failed", "mode": "live",
                    "error": note, "duration_seconds": round(time.time() - t0, 2)}

    except Exception as e:
        # File-based replays raise through; live failures are logged above.
        raise


def get_sync_status() -> dict:
    """Returns the latest sync status and checks if data is stale (> 24 hours)."""
    last_sync = sync_logs.find_one(sort=[("run_timestamp", -1)])
    if not last_sync:
        return {
            "latest_sync_timestamp": None,
            "latest_sync_status": "none",
            "is_data_stale": True,
            "staleness_message": "No sync records found. Please trigger an initial sync.",
            "rows_processed": 0
        }

    now = now_utc()
    sync_time = last_sync["run_timestamp"]
    if sync_time.tzinfo is None:
        sync_time = sync_time.replace(tzinfo=timezone.utc)

    age_hours = (now - sync_time).total_seconds() / 3600.0
    is_stale = age_hours > 24.0 or last_sync["status"] == "failed"

    if last_sync["status"] == "failed":
        msg = f"Data sync failed ({last_sync.get('error_message')}). Displaying last-known-good dataset."
    elif is_stale:
        msg = f"Data is stale (last synced {round(age_hours, 1)} hours ago)."
    else:
        msg = f"Data is fresh and synchronized ({round(age_hours, 1)}h ago)."

    return {
        "latest_sync_timestamp": last_sync["run_timestamp"].isoformat(),
        "latest_sync_status": last_sync["status"],
        "latest_sync_source": last_sync.get("source"),
        "is_data_stale": is_stale,
        "staleness_message": msg,
        "rows_processed": last_sync.get("rows_processed", 0),
        "source": last_sync.get("source"),
        "error_message": last_sync.get("error_message"),
    }
