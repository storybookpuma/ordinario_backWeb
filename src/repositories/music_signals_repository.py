from datetime import datetime


class MusicSignalsRepository:
    def __init__(self, mongo):
        self.collection = mongo.db.music_signals

    def ensure_indexes(self):
        self.collection.create_index([("userId", 1), ("occurredAt", -1)])
        self.collection.create_index([("userId", 1), ("signalType", 1), ("entityType", 1)])
        self.collection.create_index([("entityType", 1), ("entityId", 1)])

    def create(self, user_id, source, signal_type, entity_type, entity_id=None, spotify_id=None, strength=1, metadata=None, import_batch_id=None, occurred_at=None):
        doc = {
            "userId": str(user_id),
            "source": source,
            "signalType": signal_type,
            "entityType": entity_type,
            "entityId": entity_id,
            "spotifyId": spotify_id,
            "strength": strength,
            "metadata": metadata or {},
            "importBatchId": import_batch_id,
            "occurredAt": occurred_at or datetime.utcnow(),
            "createdAt": datetime.utcnow(),
        }
        return self.collection.insert_one(doc)

    def upsert(self, user_id, source, signal_type, entity_type, entity_id=None, spotify_id=None, strength=1, metadata=None, import_batch_id=None, occurred_at=None):
        now = datetime.utcnow()
        doc = {
            "userId": str(user_id),
            "source": source,
            "signalType": signal_type,
            "entityType": entity_type,
            "entityId": entity_id,
            "spotifyId": spotify_id,
            "strength": strength,
            "metadata": metadata or {},
            "importBatchId": import_batch_id,
            "occurredAt": occurred_at or now,
            "updatedAt": now,
        }
        self.collection.update_one(
            {
                "userId": str(user_id),
                "source": source,
                "signalType": signal_type,
                "entityType": entity_type,
                "entityId": entity_id,
            },
            {"$set": doc, "$setOnInsert": {"createdAt": now}},
            upsert=True,
        )
        return doc

    def recent_for_user(self, user_id, limit=25):
        return list(self.collection.find({"userId": str(user_id)}).sort("occurredAt", -1).limit(limit))
