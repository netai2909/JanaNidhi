from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional, List
import secrets
import threading

from fastapi import FastAPI, Depends, Query, HTTPException, Request, status, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import PyMongoError

from backend.config import settings
from backend.database import get_db, works, review_logs, public_reviews, sync_logs, mp_allocations, users, ensure_indexes
from backend.schemas import (
    WorkListItem, WorkPaginationResponse, CasePacketResponse,
    ReviewCreateRequest, ReviewResponse, StatsOverviewResponse,
    EntityRiskStat, SyncLogResponse, MPDirectoryItem,
    EntityDirectoryResponse, MPProfileResponse, StateProfileResponse,
    BreakdownStat, CategoryStat, StatusStat, HealthResponse,
    LoginRequest, LoginResponse, PublicReviewCreateRequest, PublicReviewResponse
)
from backend.auth import (
    get_current_role, require_reviewer_role,
    hash_password, mint_token, verify_password,
    ROLE_MOSPI_REVIEWER, ROLE_DISTRICT_AUDITOR, ROLE_PUBLIC_TIER
)
from backend.seeder import seed_database
from backend.services.ingestion import run_ingestion, get_sync_status, VALID_MODES
from backend.services import analytics
from model.risk_engine import generate_case_packet, load_models, RULE_DESCRIPTIONS
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

scheduler = BackgroundScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: ensure indexes, load risk models, and bootstrap the database.
    ensure_indexes()
    load_models(settings.MODEL_DIR)
    seed_database()

    if not settings.IS_SERVERLESS:
        # Daily live sync at 19:00 Indian Standard Time (7:00 PM IST).
        # Serverless platforms freeze the process between requests, so there
        # the sync is invoked by the platform cron (GET /api/cron/sync) instead.
        scheduler.add_job(
            run_ingestion,
            CronTrigger(hour=19, minute=0, timezone="Asia/Kolkata"),
            kwargs={"mode": "live"},
            id="daily_mplads_sync",
            replace_existing=True,
        )
        scheduler.start()
        print("APScheduler started: daily MPLADS sync at 19:00 IST (7:00 PM IST).")

    yield

    if not settings.IS_SERVERLESS:
        scheduler.shutdown()
        print("APScheduler shut down.")


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="MoSPI (SIH26102) MPLADS AI Sentinel — Audit & Anomaly Prioritization Platform",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1024)


# ==============================================================================
# 1. GET /works — Priority Queue & Filtered List
# ==============================================================================
@app.get("/works", response_model=WorkPaginationResponse)
@app.get("/api/works", response_model=WorkPaginationResponse)
def get_works(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(25, ge=1, le=100, description="Items per page"),
    state: Optional[str] = Query(None, description="Filter by State"),
    mp_name: Optional[str] = Query(None, description="Filter by MP Name"),
    house: Optional[str] = Query(None, description="Filter by House: 'Lok Sabha' or 'Rajya Sabha'"),
    ida: Optional[str] = Query(None, description="Filter by Implementing Agency"),
    risk_tier: Optional[str] = Query(None, description="Filter by Risk Tier"),
    work_category: Optional[str] = Query(None, description="Filter by Work Category"),
    work_status: Optional[str] = Query(None, description="Filter by Execution Status"),
    search: Optional[str] = Query(None, description="Search by Work ID, vendor or description"),
    sort_by: str = Query("priority_rank", description="Sort field"),
    order: str = Query("asc", description="Sort direction: 'asc' or 'desc'"),
    db=Depends(get_db),
    user_role: str = Depends(get_current_role)
):
    """
    Returns a paginated list of works ordered by priority_rank (default ascending = highest priority first).
    Supports database-level filtering and sorting over 79k+ records.
    """
    if sort_by not in analytics.WORK_SORTABLE_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sort field '{sort_by}'. Allowed: {sorted(analytics.WORK_SORTABLE_FIELDS)}"
        )
    if order.lower() not in {"asc", "desc"}:
        raise HTTPException(status_code=400, detail="order must be 'asc' or 'desc'.")

    filt = analytics.apply_work_filters(
        state=state, mp_name=mp_name, house=house, ida=ida, risk_tier=risk_tier,
        work_category=work_category, work_status=work_status, search=search,
    )

    total = works.count_documents(filt)

    direction = DESCENDING if order.lower() == "desc" else ASCENDING
    cursor = works.find(filt).sort([(sort_by, direction), ("work_id", ASCENDING)])

    offset = (page - 1) * page_size
    items_raw = cursor.skip(offset).limit(page_size)

    items = []
    for w in items_raw:
        item = analytics.work_to_list_item(w)
        flags = item["rule_flags_triggered"]
        causes = [RULE_DESCRIPTIONS.get(f, f"Flag triggered: {f}") for f in flags]
        if w.get("is_anomaly"):
            causes.append("Consensus anomaly: multiple specialist agents independently flagged this work.")
        item["causes"] = causes
        items.append(WorkListItem(**item))

    total_pages = (total + page_size - 1) // page_size if total > 0 else 1

    return WorkPaginationResponse(
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        items=items
    )


# ==============================================================================
# 1b. GET /api/export/works — Open-data CSV export (transparency feature)
# ==============================================================================
@app.get("/api/export/works")
def export_works_csv(
    state: Optional[str] = Query(None),
    mp_name: Optional[str] = Query(None),
    house: Optional[str] = Query(None),
    ida: Optional[str] = Query(None),
    risk_tier: Optional[str] = Query(None),
    work_category: Optional[str] = Query(None),
    work_status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    row_limit: int = Query(50000, ge=1, le=100000),
    db=Depends(get_db),
    user_role: str = Depends(get_current_role)
):
    """
    Streams the filtered works list as a downloadable CSV (up to row_limit rows).
    Open-data companion to the /works endpoint — same filters, machine-readable.
    """
    filt = analytics.apply_work_filters(
        state=state, mp_name=mp_name, house=house, ida=ida, risk_tier=risk_tier,
        work_category=work_category, work_status=work_status, search=search,
    )
    filename = f"mplads_works_export_{datetime.now(timezone.utc):%Y%m%d}.csv"
    return StreamingResponse(
        analytics.stream_works_csv(filt, row_limit=row_limit),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Row-Limit": str(row_limit),
        }
    )


# ==============================================================================
# 2. GET /works/{work_id} — Case Packet Detail View
# ==============================================================================
@app.get("/works/{work_id:path}", response_model=CasePacketResponse)
@app.get("/api/works/{work_id:path}", response_model=CasePacketResponse)
def get_work_case_packet(
    work_id: str,
    db=Depends(get_db),
    user_role: str = Depends(get_current_role)
):
    """
    Returns the complete case packet for the requested Work ID using risk_engine.generate_case_packet().
    """
    work = works.find_one({"work_id": work_id.strip()}, {"_id": 0})
    if not work:
        raise HTTPException(status_code=404, detail=f"Work ID '{work_id}' not found.")

    # The document already carries exactly the fields the authoritative
    # risk_engine function consumes (field names are unchanged from SQL).
    work_dict = {k: v for k, v in work.items() if not k.startswith("_")}

    packet = generate_case_packet(work_id, work_row=work_dict)

    # Fetch prior reviews for this work
    prior_reviews = review_logs.find({"work_id": work_id}).sort([("created_at", DESCENDING)])
    packet['prior_reviews'] = [
        {
            'id': r.get("id"),
            'reviewer_name': r.get("reviewer_name"),
            'reviewer_role': r.get("reviewer_role"),
            'outcome': r.get("outcome"),
            'notes': r.get("notes") if user_role != ROLE_PUBLIC_TIER else None,
            'created_at': r["created_at"].isoformat() if r.get("created_at") else None
        }
        for r in prior_reviews
    ]

    # Fetch public feedback reviews
    pub_reviews = public_reviews.find({"work_id": work_id}).sort([("created_at", DESCENDING)])
    packet['public_reviews'] = [
        {
            'id': pr.get("id"),
            'is_completed': pr.get("is_completed", False),
            'comment': pr.get("comment"),
            'photo_proof': pr.get("photo_proof"),
            'reporter_name': pr.get("reporter_name", "Anonymous Citizen"),
            'created_at': pr["created_at"].isoformat() if pr.get("created_at") else None
        }
        for pr in pub_reviews
    ]

    return CasePacketResponse(**packet)


# ==============================================================================
# 3. MP & State Transparency Directories (citizen-facing aggregate views)
# ==============================================================================
@app.get("/api/mps", response_model=EntityDirectoryResponse)
def get_mp_directory(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    state: Optional[str] = Query(None, description="Filter MPs by State"),
    house: Optional[str] = Query(None, description="Filter by House: 'Lok Sabha' or 'Rajya Sabha'"),
    search: Optional[str] = Query(None, description="Search by MP or constituency name"),
    sort_by: str = Query("total_sanctioned", description="Aggregate sort key"),
    order: str = Query("desc"),
    db=Depends(get_db),
    user_role: str = Depends(get_current_role)
):
    """MP-wise directory: sanctioned/disbursed totals, utilization, risk profile."""
    if sort_by not in analytics.DIRECTORY_SORTABLE_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sort field '{sort_by}'. Allowed: {sorted(analytics.DIRECTORY_SORTABLE_FIELDS)}"
        )
    if order.lower() not in {"asc", "desc"}:
        raise HTTPException(status_code=400, detail="order must be 'asc' or 'desc'.")
    return analytics.get_mp_directory(
        db, page=page, page_size=page_size, search=search, state=state,
        house=house, sort_by=sort_by, order=order,
    )


@app.get("/api/mps/{mp_name}", response_model=MPProfileResponse)
def get_mp_profile(
    mp_name: str,
    house: Optional[str] = Query(None, description="Filter by House: 'Lok Sabha' or 'Rajya Sabha'"),
    db=Depends(get_db),
    user_role: str = Depends(get_current_role)
):
    """Full public dossier for one MP: funds, risk tiers, categories, vendors, top works."""
    profile = analytics.get_mp_profile(db, mp_name, house=house)
    if not profile:
        raise HTTPException(status_code=404, detail=f"No works found for MP '{mp_name}'.")
    return profile


@app.get("/api/states", response_model=EntityDirectoryResponse)
def get_state_directory(
    page: int = Query(1, ge=1),
    page_size: int = Query(40, ge=1, le=100),
    house: Optional[str] = Query(None, description="Filter by House"),
    sort_by: str = Query("total_sanctioned"),
    order: str = Query("desc"),
    db=Depends(get_db),
    user_role: str = Depends(get_current_role)
):
    """State-wise directory: funds, MP coverage and risk concentration."""
    if sort_by not in analytics.DIRECTORY_SORTABLE_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sort field '{sort_by}'. Allowed: {sorted(analytics.DIRECTORY_SORTABLE_FIELDS)}"
        )
    if order.lower() not in {"asc", "desc"}:
        raise HTTPException(status_code=400, detail="order must be 'asc' or 'desc'.")
    return analytics.get_state_directory(db, page=page, page_size=page_size, house=house, sort_by=sort_by, order=order)


@app.get("/api/states/{state}", response_model=StateProfileResponse)
def get_state_profile(
    state: str,
    db=Depends(get_db),
    user_role: str = Depends(get_current_role)
):
    """State dossier: tier spread, top MPs, agencies and category splits."""
    profile = analytics.get_state_profile(db, state)
    if not profile:
        raise HTTPException(status_code=404, detail=f"No works found for state '{state}'.")
    return profile


# ==============================================================================
# 3b. Chart analytics — category & execution-status aggregations
# ==============================================================================
@app.get("/api/analytics/categories", response_model=List[CategoryStat])
def get_category_analytics(
    house: Optional[str] = Query(None, description="Filter by House"),
    db=Depends(get_db),
    user_role: str = Depends(get_current_role)
):
    """Fund share, disbursed value and risk per work category."""
    return analytics.get_category_analytics(db, house=house)


@app.get("/api/analytics/status", response_model=List[StatusStat])
def get_status_analytics(
    house: Optional[str] = Query(None, description="Filter by House"),
    db=Depends(get_db),
    user_role: str = Depends(get_current_role)
):
    """Execution status distribution with average risk per status."""
    return analytics.get_status_analytics(db, house=house)


# ==============================================================================
# 4. GET /stats/overview — Portfolio Statistics & Sync Health
# ==============================================================================
@app.get("/stats/overview", response_model=StatsOverviewResponse)
@app.get("/api/stats/overview", response_model=StatsOverviewResponse)
def get_stats_overview(
    house: Optional[str] = Query(None, description="Filter by House: 'Lok Sabha' or 'Rajya Sabha'"),
    db=Depends(get_db)
):
    """
    Returns portfolio-level statistics including risk tier counts, top-risk MPs,
    top-risk states, top-risk vendors, and sync health / staleness status.
    """
    def scoped(filt: dict) -> dict:
        return analytics.apply_house(filt, house) if house else filt

    total_works = works.count_documents(scoped({}))
    high_risk_count = works.count_documents(scoped({"risk_tier": 'High Risk - Review'}))
    medium_risk_count = works.count_documents(scoped({"risk_tier": 'Medium Risk - Monitor'}))
    low_risk_count = works.count_documents(scoped({"risk_tier": 'Low Risk'}))

    def _scalar(stage_op: str, field: str, extra_match: Optional[dict] = None) -> float:
        match = scoped(extra_match or {})
        rows = works.aggregate([
            {"$match": match},
            {"$group": {"_id": None, "v": {stage_op: {"$ifNull": [f"${field}", 0.0]}}}},
        ])
        row = next(rows, None)
        return float(row["v"]) if row and row["v"] is not None else 0.0

    total_sanctioned = _scalar("$sum", "sanction_amount")
    total_disbursed = _scalar("$sum", "total_fund_disbursed")
    avg_utilization = _scalar("$avg", "utilization_ratio")
    avg_risk = _scalar("$avg", "final_risk_score")
    reviewed_count = works.count_documents(scoped({"human_review_outcome": {"$ne": None}}))

    # Portal allocation ledger (Total Allocated / utilization / ongoing payments).
    # MPAllocation is filtered on its own house field — it is an MP-level
    # ledger, not a per-work one.
    alloc_match: dict = {}
    if house:
        alloc_match["house"] = house.strip()
    alloc_rows = mp_allocations.aggregate([
        {"$match": alloc_match},
        {"$group": {"_id": None, "total": {"$sum": {"$ifNull": ["$allocated_amount", 0.0]}}}},
    ])
    alloc_row = next(alloc_rows, None)
    total_allocated = float(alloc_row["total"]) if alloc_row and alloc_row["total"] is not None else 0.0

    works_completed = works.count_documents(
        scoped({"completion_date": {"$nin": [None, ""]}})
    )
    works_pending = max(0, total_works - works_completed)

    # Ongoing work payments: sum of disbursed funds for works that are NOT yet completed
    ongoing_payments = _scalar("$sum", "total_fund_disbursed", extra_match={
        "$or": [{"completion_date": None}, {"completion_date": ""}],
        "total_fund_disbursed": {"$gt": 0},
    })

    tier_dist = {
        'High Risk - Review': high_risk_count,
        'Medium Risk - Monitor': medium_risk_count,
        'Low Risk': low_risk_count,
    }

    top_states = [EntityRiskStat(**s) for s in analytics.top_entity_stats("state", house)]
    top_mps = [EntityRiskStat(**m) for m in analytics.top_entity_stats("mp_name", house)]
    top_vendors = [EntityRiskStat(**v) for v in analytics.top_entity_stats("primary_vendor", house)]

    # Sync freshness
    sync_info = get_sync_status()

    return StatsOverviewResponse(
        total_works=total_works,
        high_risk_count=high_risk_count,
        medium_risk_count=medium_risk_count,
        low_risk_count=low_risk_count,
        tier_distribution=tier_dist,
        total_sanctioned_amount=round(float(total_sanctioned), 2),
        total_disbursed_amount=round(float(total_disbursed), 2),
        total_allocated_amount=round(float(total_allocated), 2),
        fund_utilization_pct=round(float(total_sanctioned) / float(total_allocated) * 100, 1) if total_allocated else 0.0,
        expenditure_rate_pct=round(float(total_disbursed) / float(total_allocated) * 100, 1) if total_allocated else 0.0,
        works_completed=int(works_completed),
        works_pending=int(works_pending),
        ongoing_work_payments=round(float(ongoing_payments), 2),
        avg_utilization_pct=round(float(avg_utilization) * 100, 1),
        avg_risk_score=round(float(avg_risk), 1),
        reviewed_works=int(reviewed_count),
        top_risk_mps=top_mps,
        top_risk_states=top_states,
        top_risk_vendors=top_vendors,
        latest_sync_timestamp=sync_info["latest_sync_timestamp"],
        latest_sync_status=sync_info["latest_sync_status"],
        is_data_stale=sync_info["is_data_stale"],
        staleness_message=sync_info["staleness_message"]
    )


# ==============================================================================
# 5. POST /works/{work_id}/review — Human Review Feedback Loop
# ==============================================================================
@app.post("/works/{work_id:path}/review", response_model=ReviewResponse)
@app.post("/api/works/{work_id:path}/review", response_model=ReviewResponse)
def record_human_review(
    work_id: str,
    payload: ReviewCreateRequest,
    db=Depends(get_db),
    user_role: str = Depends(require_reviewer_role)
):
    """
    Phase 5 feedback loop hook: Record a formal human review outcome.
    Allowed outcomes: 'legitimate', 'data-quality issue', 'irregularity', 'confirmed fraud'.
    """
    valid_outcomes = {'legitimate', 'data-quality issue', 'irregularity', 'confirmed fraud'}
    norm_outcome = payload.outcome.strip().lower()
    if norm_outcome not in valid_outcomes:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid review outcome '{payload.outcome}'. Must be one of: {list(valid_outcomes)}"
        )

    work_id = work_id.strip()
    if not works.find_one({"work_id": work_id}):
        raise HTTPException(status_code=404, detail=f"Work ID '{work_id}' not found.")

    reviewer_name = payload.reviewer_name or user_role
    reviewer_role = payload.reviewer_role or user_role
    now = datetime.now(timezone.utc)

    from backend.database import next_id
    review_log = {
        "id": next_id("review_logs"),
        "work_id": work_id,
        "reviewer_name": reviewer_name,
        "reviewer_role": reviewer_role,
        "outcome": norm_outcome,
        "notes": payload.notes,
        "created_at": now,
    }
    review_logs.insert_one(review_log)

    works.update_one(
        {"work_id": work_id},
        {"$set": {"human_review_outcome": norm_outcome, "updated_at": now}}
    )

    return ReviewResponse(
        success=True,
        work_id=work_id,
        outcome=norm_outcome,
        reviewer_name=reviewer_name,
        reviewer_role=reviewer_role,
        notes=payload.notes,
        created_at=now
    )


# ==============================================================================
# 5a. POST /works/{work_id}/public-review — Citizen Public Verification Feedback
# ==============================================================================
@app.post("/works/{work_id:path}/public-review", response_model=PublicReviewResponse)
@app.post("/api/works/{work_id:path}/public-review", response_model=PublicReviewResponse)
def record_public_review(
    work_id: str,
    payload: PublicReviewCreateRequest,
    db=Depends(get_db)
):
    """
    Public Tier feedback endpoint: Allows citizens to report whether a work is completed or not,
    along with comments and photo proof.
    """
    work_id = work_id.strip()
    if not works.find_one({"work_id": work_id}):
        raise HTTPException(status_code=404, detail=f"Work ID '{work_id}' not found.")

    now = datetime.now(timezone.utc)
    from backend.database import next_id

    doc = {
        "id": next_id("public_reviews"),
        "work_id": work_id,
        "is_completed": bool(payload.is_completed),
        "comment": payload.comment,
        "photo_proof": payload.photo_proof,
        "reporter_name": payload.reporter_name or "Anonymous Citizen",
        "created_at": now
    }
    public_reviews.insert_one(doc)

    return PublicReviewResponse(
        success=True,
        work_id=work_id,
        is_completed=doc["is_completed"],
        comment=doc["comment"],
        photo_proof=doc["photo_proof"],
        reporter_name=doc["reporter_name"],
        created_at=now
    )


# ==============================================================================
# 5b. POST /api/auth/login — MongoDB Authentication Endpoint
# ==============================================================================
@app.post("/auth/login", response_model=LoginResponse)
@app.post("/api/auth/login", response_model=LoginResponse)
def login_user(payload: LoginRequest, db=Depends(get_db)):
    """
    Authenticates District Auditor and MoSPI Reviewer users against the MongoDB
    `users` collection and returns a signed token carrying the caller's role.
    Present it as `Authorization: Bearer <token>` on elevated requests.
    """
    uname = payload.username.strip()

    user = users.find_one({"username": uname})
    if not user or not verify_password(payload.password, user):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Wrong username or password"
        )

    # Upgrade legacy plaintext rows to salted hashes after a successful login.
    if user.get("password") and not user.get("password_hash"):
        users.update_one(
            {"_id": user["_id"]},
            {"$set": {"password_hash": hash_password(payload.password)},
             "$unset": {"password": ""}},
        )

    assigned_role = payload.target_role or user.get("role") or ROLE_MOSPI_REVIEWER
    if assigned_role not in {ROLE_MOSPI_REVIEWER, ROLE_DISTRICT_AUDITOR}:
        assigned_role = ROLE_MOSPI_REVIEWER

    return LoginResponse(
        success=True,
        username=user["username"],
        role=assigned_role,
        token=mint_token(user["username"], assigned_role),
        message="Authentication successful"
    )


# ==============================================================================
# 6. Filter options, Sync Health, Manual Trigger & Health
# ==============================================================================
@app.get("/api/filter-options")
def get_filter_options(
    house: Optional[str] = Query(None, description="Filter options by House"),
    db=Depends(get_db)
):
    """Returns unique filter values for the frontend dropdowns."""
    house_filter = {"house": house.strip()} if house else {}

    def _sorted_distinct(field: str) -> list:
        values = works.distinct(field, house_filter)
        return sorted(v for v in values if v)

    return {
        "states": _sorted_distinct("state"),
        "categories": _sorted_distinct("work_category"),
        "statuses": _sorted_distinct("work_status"),
        "mps": _sorted_distinct("mp_name"),
        "risk_tiers": ["High Risk - Review", "Medium Risk - Monitor", "Low Risk"]
    }


@app.get("/sync/status")
@app.get("/api/sync/status")
def sync_status():
    return get_sync_status()


@app.get("/sync/logs", response_model=List[SyncLogResponse])
@app.get("/api/sync/logs", response_model=List[SyncLogResponse])
def get_sync_logs(
    limit: int = 20,
    db=Depends(get_db)
):
    logs = sync_logs.find({}, {"_id": 0}).sort([("run_timestamp", DESCENDING)]).limit(limit)
    return [SyncLogResponse(**doc) for doc in logs]


_sync_in_flight = threading.Lock()


def _cron_authorized(request: Request) -> bool:
    """Platform-cron authentication: Vercel Cron sends
    `Authorization: Bearer $CRON_SECRET` when a CRON_SECRET env var exists."""
    if not settings.CRON_SECRET:
        return False
    auth_header = request.headers.get("authorization", "")
    cron_header = request.headers.get("x-cron-secret", "")
    return (
        secrets.compare_digest(auth_header, f"Bearer {settings.CRON_SECRET}")
        or secrets.compare_digest(cron_header, settings.CRON_SECRET)
    )


def _start_background_sync(mode: str) -> dict:
    if mode not in VALID_MODES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid ingestion mode '{mode}'. Must be one of {sorted(VALID_MODES)}"
        )
    if not _sync_in_flight.acquire(blocking=False):
        raise HTTPException(
            status_code=409,
            detail="A sync is already running. Watch the status banner — the "
                   "new run will appear in the audit log when it finishes."
        )

    def _run():
        try:
            result = run_ingestion(mode=mode)
            print(f"Background sync finished: {result.get('status')} — "
                  f"{result.get('processed', 0)} records processed.")
        except Exception as e:
            print(f"Background sync crashed: {e}")
        finally:
            _sync_in_flight.release()

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "started", "mode": mode,
            "message": "Live sync started in the background. "
                       "Progress appears in the audit log when it finishes."}


@app.post("/sync/run")
@app.post("/api/sync/run")
def trigger_manual_sync(
    request: Request,
    mode: str = Query("auto", description="Ingestion mode: auto | live"),
    user_role: str = Depends(get_current_role)
):
    """
    Manually trigger the ingestion pipeline. Restricted to MoSPI Reviewers;
    platform crons are also accepted with the CRON_SECRET credential.

    Runs in the background — the full portal fetch + risk-score of the Lok
    Sabha dataset takes several minutes, so the request returns immediately.
    Progress lands in sync_logs and the /api/sync/status endpoint; the UI
    polls that banner.

    Modes:
      auto / live — pull directly from the live MPLADS dashboard API
                    (mplads.mospi.gov.in /digigov), risk-score, and upsert.
    """
    if not (_cron_authorized(request) or user_role == ROLE_MOSPI_REVIEWER):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access forbidden: Only MoSPI Reviewers can perform governance and sync operations."
        )
    return _start_background_sync(mode)


@app.get("/cron/sync")
@app.get("/api/cron/sync")
def cron_sync(
    request: Request,
    mode: str = Query("auto", description="Ingestion mode: auto | live"),
):
    """
    Platform-cron entry point (Vercel Cron issues GET requests, so the POST
    /sync/run path cannot be used). Runs the ingestion INLINE so the platform
    billing/timeout envelope covers the whole run — chunked upserts mean a
    timeout leaves partial progress in place rather than losing the run.
    """
    if not _cron_authorized(request):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access forbidden: invalid cron credentials."
        )
    if mode not in VALID_MODES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid ingestion mode '{mode}'. Must be one of {sorted(VALID_MODES)}"
        )
    return run_ingestion(mode=mode)


@app.get("/health")
@app.get("/api/health", response_model=HealthResponse)
def health_check(db=Depends(get_db)):
    """Liveness probe: verifies API + database connectivity."""
    try:
        works_count = works.count_documents({})
        db_status = "connected"
    except PyMongoError:
        works_count = 0
        db_status = "unavailable"
    return HealthResponse(
        status="ok" if db_status == "connected" else "degraded",
        version=settings.VERSION,
        database=db_status,
        works_count=int(works_count),
        timestamp=datetime.now(timezone.utc).isoformat()
    )


# In the Docker container (Hugging Face), FastAPI serves the production React
# bundle so the API and dashboard share one public origin. On Vercel the
# frontend is served by the static build instead.
_frontend_dist = settings.DATA_DIR.parent / "frontend" / "dist"
if _frontend_dist.is_dir():
    app.mount("/", StaticFiles(directory=_frontend_dist, html=True), name="frontend")
