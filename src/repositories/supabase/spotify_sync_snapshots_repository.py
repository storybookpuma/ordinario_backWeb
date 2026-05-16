from datetime import datetime, timezone


class SupabaseSpotifySyncSnapshotsRepository:
    table = "spotify_sync_snapshots"

    def __init__(self, client):
        self.client = client

    def create(self, user_id, sync_type, status="running"):
        return self.client.insert_one(self.table, {
            "user_id": str(user_id),
            "sync_type": sync_type,
            "status": status,
            "started_at": datetime.now(timezone.utc).isoformat(),
        })

    def complete(self, snapshot_id, items_processed):
        return self.client.update(self.table, {"id": str(snapshot_id)}, {
            "status": "completed",
            "items_processed": items_processed,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })

    def fail(self, snapshot_id, error_message):
        return self.client.update(self.table, {"id": str(snapshot_id)}, {
            "status": "failed",
            "error_message": error_message,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })
