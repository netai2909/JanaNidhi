---
title: MPLADS AI Sentinel
sdk: docker
app_port: 7860
---

# MPLADS AI Sentinel — Audit & Anomaly Prioritization Platform
**MoSPI (SIH26102)** — Ministry of Statistics and Programme Implementation

---

## 1. What MPLADS AI Sentinel Does

**MPLADS AI Sentinel** is an AI-assisted audit prioritization and decision-support web platform designed for **MoSPI**. It systematically surfaces potential irregularities, cost outliers, duplicate claims, stagnant implementation, and procurement concentration across works executed under the **Members of Parliament Local Area Development Scheme (MPLADS)**.

> **Crucial Explainability Principle:**  
> The system produces **Risk Tiers (`High Risk - Review`, `Medium Risk - Monitor`, `Low Risk`)** and **Action Directives**, not definitive criminal verdicts. An AI flag is a decision-support filter for audit inspection; only authorized human reviewers can confirm an irregularity or fraud.

---

## 2. Project Architecture

```
project-root/
├── docs/
│   ├── SRS.md                      # Full Software Requirements Specification
│   └── fraud_detection_logic.md    # Evidence-grounded typology & signal mapping
│
├── model/
│   ├── risk_engine.py              # Authoritative risk engine implementation
│   └── agents/                     # Five specialist agents + coordinator (deterministic, no ML artifacts)
│
├── data/
│   ├── mplads_raw_sample.csv       # Reference schema sample for tests/offline replay
│   └── last_live_feed.csv          # Local live-sync cache (not required for deployment)
│
├── backend/
│   ├── config.py                   # Environment configuration (.env loader)
│   ├── database.py                 # MongoDB connection, collections & indexes
│   ├── models.py                   # Mongo document field helpers
│   ├── schemas.py                  # Pydantic REST API schemas
│   ├── auth.py                     # Signed login tokens, PBKDF2 password hashing, RBAC (fail-closed)
│   ├── seeder.py                   # Idempotent bootstrap seeder (sample feed + admin user)
│   ├── main.py                     # FastAPI REST API application
│   └── services/
│       ├── ingestion.py            # Scheduled ingestion & sync logging
│       ├── analytics.py            # MP/state directories, chart analytics, CSV export
│       └── mplads_live.py          # Live MPLADS dashboard API client
│
├── frontend/                       # React + Tailwind CSS Dashboard
│   ├── src/
│   │   ├── components/
│   │   │   ├── ui/                 # shadcn/ui primitives (button, card, dialog, select,
│   │   │   │                       #   table, tabs, badge, input, progress, skeleton,
│   │   │   │                       #   tooltip, dropdown-menu, sonner, textarea)
│   │   │   ├── magicui/            # Magic UI motion components (number-ticker,
│   │   │   │                       #   shimmer-button, animated-shiny-text, dot-pattern,
│   │   │   │                       #   border-beam, blur-fade)
│   │   │   ├── Header.jsx          # MoSPI branding, status pill, role selector
│   │   │   ├── PriorityQueue.jsx   # Default view ordered by priority rank
│   │   │   ├── CasePacketModal.jsx # Detailed dossier with review feedback
│   │   │   ├── PortfolioOverview.jsx # Macro metrics, charts & entity risk ranking
│   │   │   ├── MPDirectory.jsx     # Public MP fund directory (sortable table)
│   │   │   ├── MPProfileModal.jsx  # Per-MP transparency dossier
│   │   │   ├── SyncLogsView.jsx    # Audit logs and governance disclosure
│   │   │   └── ...
│   │   ├── lib/                    # cn() utility + INR/percentage formatting
│   │   ├── App.jsx                 # Main stateful application
│   │   └── index.css               # shadcn design tokens (Tailwind v4) + glass system
│   ├── components.json             # shadcn CLI configuration
│   └── vite.config.js              # Vite configuration, @ alias & backend proxy
│
└── tests/
    ├── test_risk_engine.py         # Unit tests for models & zero-drift scoring
    └── test_api.py                 # Integration tests for endpoints & RBAC
```

---

## 2b. Public Transparency API (v1.1 additions)

Inspired by civic-transparency platforms, the backend now exposes aggregate, citizen-facing
views alongside the auditor queue (all under `/api`):

| Endpoint | Purpose |
|---|---|
| `GET /api/mps` | Paginated MP directory: sanctioned/disbursed totals, utilization, avg & max risk, high-risk counts. Supports `search`, `state`, `sort_by`, `order`. |
| `GET /api/mps/{mp_name}` | Full MP dossier: tier split, category/status/agency/vendor breakdowns, top-risk works, recent reviews. |
| `GET /api/states` | State-wise aggregation incl. MP coverage counts. |
| `GET /api/states/{state}` | State dossier: tier spread, top MPs, category & agency breakdowns. |
| `GET /api/analytics/categories` | Fund share + risk per work category (chart feed). |
| `GET /api/analytics/status` | Execution-status distribution (chart feed). |
| `GET /api/export/works` | **Open-data CSV export** — streams the filtered works list (same filters as `/works`, `row_limit` capped at 100k). |
| `GET /api/health` | Liveness probe: API + database status, record count. |

Hardening & UX changes shipped with v1.1:
- **Fail-closed RBAC** — missing or unknown `X-User-Role` headers now degrade to *Read-Only Public Tier* instead of escalating to MoSPI Reviewer.
- **Sort-field whitelist** on `/works` and directory endpoints (invalid fields return `400`).
- **GZip middleware** for compressed responses; new `/api/works` filter `work_status`.
- `/api/filter-options` now also returns `statuses` and the full `mps` list.
- Frontend additions: **MP Fund Directory** tab with profile dossiers, category/status charts on the overview, active-filter chips, debounced search, and one-click CSV export of the current filtered view.

### UI system (shadcn/ui + Magic UI)

The dashboard was redesigned on the [shadcn/ui](https://github.com/shadcn-ui/ui) component
system with [Magic UI](https://github.com/magicuidesign/magicui) motion accents, following the
same copy-in component model used by Spectrum UI and shadcnblocks:

- **Toolchain:** `components.json` + `@` path alias + `cn()` utility; Tailwind v4 CSS-first
  design tokens in `src/index.css`.
- **Primitives (`src/components/ui/`):** vendored via `npx shadcn@latest add …` — button,
  badge, card, dialog, input, select, table, tabs, tooltip, progress, skeleton,
  dropdown-menu, separator, sonner (toast), textarea. Radix-powered accessible dialogs
  with focus trapping; review errors surface as toasts instead of `alert()`.
- **Motion (`src/components/magicui/`):** `NumberTicker` (animated stat counters, en-IN
  grouping), `ShimmerButton` (primary CTAs), `AnimatedShinyText` (header tagline),
  `DotPattern` (app backdrop), `BorderBeam` (high-risk metric accent), `BlurFade`
  (list entrance animations).
- To add more components later: `npx shadcn@latest add <component>` (primitives) or copy
  from the Magic UI catalog into `src/components/magicui/`.

### Soft light theme, typography & charts

The visual language follows the **ui-ux-pro-max** design-intelligence skill (installed at
`.zcode/skills/ui-ux-pro-max`), queried for a Government/Public transparency dashboard:

- **Style:** Minimalism & Swiss style + accessible design — soft light surfaces
  (slate-50 `#F8FAFC` background, white cards, slate-200 borders), professional indigo
  primary (`#4F46E5`, 4.7:1 on white), WCAG-safe tint chips for risk tiers.
- **Fonts:** *Outfit* (display headings) + *Work Sans* (body) — the "Geometric Modern"
  pairing from the skill's typography database — with *Fira Code* for tabular figures.
- **Charts:** [Chart.js](https://www.chartjs.org) via `react-chartjs-2`
  (`src/lib/chart.js` holds the shared registration + light-theme tooltip defaults):
  an execution-status doughnut (≤6 slices, largest at 12 o'clock, exact values in the
  legend — per the skill's part-to-whole guidance) and click-to-filter horizontal
  category bars on the Portfolio Overview, plus a per-MP category bar in the profile
  dossier.

---

## 3. How to Install Dependencies

### Prerequisites
- Python 3.11+ (Python 3.14 supported)
- Node.js 18+ and npm

### Backend Installation
```bash
# In the project root:
py -m pip install -r requirements.txt
# Or install core packages:
py -m pip install fastapi uvicorn apscheduler pandas numpy pymongo dnspython pytest httpx requests
```

### Frontend Installation
```bash
cd frontend
npm install
```

---

## 4. How to Configure MongoDB

The backend uses MongoDB as its only persistence layer (a free MongoDB Atlas M0 cluster is enough). Set the connection URL in your `.env` file:
```env
MONGODB_URI=mongodb+srv://user:password@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority
MONGO_DB_NAME=mplads_sentinel
```
*(Note: For local development, a plain `mongod` on the default `mongodb://localhost:27017` is picked up automatically).*

---

## 5. How to Configure Environment Variables

Copy the example template:
```bash
cp .env.example .env
```
Key configuration parameters:
- `MONGODB_URI`: MongoDB connection string.
- `MONGO_DB_NAME`: Database name (default `mplads_sentinel`).
- `API_PREFIX`: `/api`
- `CORS_ORIGINS`: Allowed web client origins.
- `ADMIN_USERNAME` / `ADMIN_PASSWORD`: bootstrap admin credentials (seeded only when absent; passwords are stored as salted hashes).
- `AUTH_SECRET`: HMAC key that signs login tokens — set a strong random value in production.
- Daily sync is automatic at **19:00 IST (7:00 PM IST)** (APScheduler cron); no configuration needed.

---

## 6. How to Seed the Database

On application startup, the backend automatically verifies if the `works` collection is empty. If empty, it starts a live sync from the MPLADS dashboard in the background:
```bash
py -m backend.seeder
```
- **Idempotent:** Safe to run repeatedly; skips execution if records already exist.
- The API becomes available immediately while the initial live sync runs.

---

## 7. How to Start the Backend

Start the FastAPI application with Uvicorn:
```bash
py -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```
API Documentation will be available at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

---

## 8. How to Start the Frontend

In a separate terminal:
```bash
cd frontend
npm run dev
```
Open your browser at `http://localhost:5173`.

---

## 9. How to Run the Ingestion Job Manually

Sync is **mode-based**. Trigger via HTTP (requires `MoSPI Reviewer` role):

```bash
# auto / live — pull fresh data from the MPLADS portal API (runs in the background)
curl -X POST "http://localhost:8000/api/sync/run?mode=live" -H "X-User-Role: MoSPI Reviewer"
```

The daily run happens automatically at **19:00 IST (7:00 PM IST)**. Every run writes exactly one
`sync_logs` entry (LIVE API badge) and the failure reason is preserved in the log.

---

## 10. How Scheduled Ingestion Works

1. **Scheduler:** An `APScheduler` cron job runs a **live sync automatically at 19:00 IST (7:00 PM IST)**
   every night. Manual triggers are available via `POST /api/sync/run?mode=live`, which
   starts the run in the background and returns immediately.
2. **Single source — the live portal:**
   - Data is fetched directly from the MPLADS dashboard's own REST endpoint
     (`mplads.mospi.gov.in/rest/PreLoginDashboardData/getTilesReportData`); houses selected
     via `MPLADS_LIVE_HOUSE` (default `both`). Implemented in `backend/services/mplads_live.py`.
   - Per-MP allotted funds from the portal's *Allocated Limit* dataset are stored in the
     `mp_allocations` table, powering Total Allocated / Fund Utilization / Expenditure Rate.
3. **Failure handling (staleness protection):**
   - If a sync fails, existing records are **never deleted**; the failure reason is written
     to `sync_logs` and the header badge shows **Data Stale** while the last-known-good
     dataset keeps serving.
   - Raw long-format feeds are reshaped and re-scored through `risk_engine.score_dataset()`.

### Remote feed configuration (`.env`)

The backend loads `.env` automatically (no extra dependency). For local/demo runs the
shipped Baseline Export is re-served over HTTP at
`GET /api/feeds/baseline-export.csv`, so the remote mode performs a genuine
fetch → parse → upsert cycle:

```env
REMOTE_INGESTION_URL=http://127.0.0.1:8001/api/feeds/baseline-export.csv
REMOTE_INGESTION_TIMEOUT=120
```

In production, point `REMOTE_INGESTION_URL` at the real official export host instead —
no code changes required. The Sync & Governance screen shows whether the remote feed is
configured and tags every sync-log entry with its feed type.

---

## 11. Authoritative Risk Engine & Unified Formula

The risk engine is located at:
```text
model/risk_engine.py
```
It is the **single source of truth** for all risk scoring and case packet generation.

### The Unified Formula:
$$\text{likelihood} = 0.45 \times \text{weighted\_rule\_score} + 0.30 \times \text{anomaly\_percentile} + 0.25 \times \text{model\_risk\_score}$$
$$\text{priority} = \text{likelihood} \times (0.5 + 0.5 \times \text{impact})$$
$$\text{final\_risk\_score} = \text{percentile\_rank}(\text{priority}) \times 100$$

- **Likelihood:** Number and severity of independent signals triggered.
- **Impact:** Financial magnitude of public funds involved.
- **Priority:** Elevates cases where risk likelihood intersects with high expenditure.

---

## 12. Pretrained Models

Both pretrained model artifacts are stored in `model/` and loaded directly without retraining:
- `model/trained_isolation_forest.pkl`: Unsupervised multi-dimensional anomaly detector (300 estimators).
- `model/trained_xgb_model.json`: Pretrained XGBoost regressor artifact.

---

## 13. How Human Review Works (Phase 5 Feedback Loop)

Authorized auditors submit verification outcomes via:
```http
POST /works/{work_id}/review
```
**Allowed Outcomes:**
- `legitimate`: Valid documentation and physical progress verified.
- `data-quality issue`: Typo, portal date error, or reporting artifact.
- `irregularity`: Substantive procedural, guideline, or cost violation.
- `confirmed fraud`: Fictitious ghost work, embezzlement, or duplicate billing.

Outcomes are recorded in PostgreSQL `review_logs` to build ground-truth labels for future supervised model retraining.

---

## 14. Known Limitations & Official Data-Source Investigation

**Live-data integration is implemented** via the MPLADS dashboard's internal REST API
(`POST /rest/PreLoginDashboardData/getTilesReportData` — session-bootstrapped from the
public dashboard page, no login required; see `backend/services/mplads_live.py`).

Remaining limitations:
1. **MPLADS Portal rate/stability:** The dashboard REST endpoint is undocumented; response
   shape and availability may change without notice. The ingestion chain (live → remote →
   baseline → sample) protects against this.
2. **Lok Sabha payload size:** The 18th Lok Sabha "Works Recommended" dataset exceeds
   90 MB; scheduled syncs default to `MPLADS_LIVE_HOUSE=rajya_sabha` for speed. Set it to
   `both` to include Lok Sabha (fetch can take several minutes).
3. **data.gov.in:** Hosts high-level summaries under NDSAP, but does not provide open real-time feeds for transaction-level work vouchers.
4. **Image Markers:** The `Image` column in portal exports is an administrative marker indicating attachment presence, not actual image binary data or perceptual hashes. Computer-vision duplicate detection is documented as a data gap until binary attachments are provided.

---

## 15. Running the Test Suite

Execute the complete automated test suite verifying model loading, zero scoring drift, API pagination, and RBAC:
```bash
py -m pytest tests -v
```
Output:
```text
tests/test_api.py::test_get_works_pagination_and_priority_order PASSED
tests/test_api.py::test_get_works_filtering PASSED
tests/test_api.py::test_get_case_packet PASSED
tests/test_api.py::test_get_stats_overview PASSED
tests/test_api.py::test_human_review_workflow_and_rbac PASSED
tests/test_risk_engine.py::test_models_load_successfully PASSED
tests/test_risk_engine.py::test_generate_case_packet PASSED
tests/test_risk_engine.py::test_risk_score_consistency_and_zero_drift PASSED

8 passed in ~3s
```
