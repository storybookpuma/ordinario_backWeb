from datetime import datetime


class SpotifySyncSnapshotsRepository:
    def __init__(self, mongo):
        self.collection = mongo.db.spotify_sync_snapshots

    def create(self, user_id, sync_type, status="running"):
        doc = {
            "userId": str(user_id),
            "syncType": sync_type,
            "status": status,
            "startedAt": datetime.utcnow(),
            "itemsProcessed": 0,
            "createdAt": datetime.utcnow(),
        }
        result = self.collection.insert_one(doc)
        doc["_id"] = result.inserted_id
        return doc

    def complete(self, snapshot_id, items_processed):
        return self.collection.update_one({"_id": snapshot_id}, {"$set": {
            "status": "completed",
            "finishedAt": datetime.utcnow(),
            "itemsProcessed": items_processed,
        }})

    def fail(self, snapshot_id, error_message):
        return self.collection.update_one({"_id": snapshot_id}, {"$set": {
            "status": "failed",
            "finishedAt": datetime.utcnow(),
            "errorMessage": error_message,
        }})
