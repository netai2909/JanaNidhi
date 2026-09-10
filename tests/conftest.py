import pymongo
from pymongo.errors import ServerSelectionTimeoutError

# Patch mongomock BulkOperationBuilder.add_replace to handle PyMongo's 'sort' argument
# and add missing $setDifference aggregation set operator support for mongomock
try:
    import mongomock
    from mongomock.collection import BulkOperationBuilder
    from mongomock.aggregate import _Parser

    _orig_add_replace = BulkOperationBuilder.add_replace
    _orig_add_update = BulkOperationBuilder.add_update

    def _patched_add_replace(self, selector, replacement, upsert=False, collation=None, hint=None, sort=None):
        return _orig_add_replace(self, selector, replacement, upsert=upsert, collation=collation, hint=hint)

    def _patched_add_update(self, selector, update, upsert=False, multi=False, collation=None, array_filters=None, hint=None, sort=None):
        return _orig_add_update(self, selector, update, upsert=upsert, multi=multi, collation=collation, array_filters=array_filters, hint=hint)

    BulkOperationBuilder.add_replace = _patched_add_replace
    BulkOperationBuilder.add_update = _patched_add_update

    _orig_handle_set = _Parser._handle_set_operator

    def _patched_handle_set(self, operator, values):
        if operator == "$setDifference":
            set1, set2 = values
            s1 = self.parse(set1) or []
            s2 = self.parse(set2) or []
            return [x for x in s1 if x not in s2]
        return _orig_handle_set(self, operator, values)

    _Parser._handle_set_operator = _patched_handle_set

except ImportError:
    mongomock = None

import backend.database as db

# Check if MongoDB is reachable
mongodb_available = False
try:
    db._client.admin.command("ping", serverSelectionTimeoutMS=1000)
    mongodb_available = True
except Exception:
    mongodb_available = False

if not mongodb_available and mongomock is not None:
    mock_client = mongomock.MongoClient()
    mock_db = mock_client["test_mplads"]

    db._client = mock_client
    db.db = mock_db
    db.works = mock_db["works"]
    db.mp_allocations = mock_db["mp_allocations"]
    db.review_logs = mock_db["review_logs"]
    db.public_reviews = mock_db["public_reviews"]
    db.users = mock_db["users"]
    db.sync_logs = mock_db["sync_logs"]
    db._counters = mock_db["counters"]

    # Also update modules that imported works/etc directly at module level
    import backend.services.analytics as analytics
    import backend.services.ingestion as ingestion

    analytics.works = mock_db["works"]
    analytics.mp_allocations = mock_db["mp_allocations"]
    analytics.review_logs = mock_db["review_logs"]

    ingestion.works = mock_db["works"]
    ingestion.mp_allocations = mock_db["mp_allocations"]
    ingestion.review_logs = mock_db["review_logs"]
    ingestion.sync_logs = mock_db["sync_logs"]
    ingestion._counters = mock_db["counters"]

    import backend.main as main
    main.works = mock_db["works"]
    main.mp_allocations = mock_db["mp_allocations"]
    main.review_logs = mock_db["review_logs"]
    main.sync_logs = mock_db["sync_logs"]
    main.public_reviews = mock_db["public_reviews"]
    main.users = mock_db["users"]

    import backend.seeder as seeder
    seeder.works = mock_db["works"]
    seeder.users = mock_db["users"]
    seeder.seed_users()

    # Seed mock db if empty
    from backend.config import settings
    if mock_db["works"].count_documents({}) == 0:
        ingestion.run_ingestion(mode="auto", source_file_path=settings.RAW_SAMPLE_PATH)
