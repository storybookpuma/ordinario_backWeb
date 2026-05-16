class SupabaseMusicSignalsRepository:
    table = "music_signals"

    def __init__(self, client):
        self.client = client

    def ensure_indexes(self):
        return None

    def create(self, user_id, source, signal_type, entity_type, entity_id=None, spotify_id=None, strength=1, metadata=None, import_batch_id=None, occurred_at=None):
        payload = {
            "user_id": str(user_id),
            "source": source,
            "signal_type": signal_type,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "spotify_id": spotify_id,
            "strength": strength,
            "metadata": metadata or {},
            "import_batch_id": import_batch_id,
        }
        if occurred_at is not None:
            payload["occurred_at"] = occurred_at
        return self.client.insert_one(self.table, payload)

    def upsert(self, user_id, source, signal_type, entity_type, entity_id=None, spotify_id=None, strength=1, metadata=None, import_batch_id=None, occurred_at=None):
        payload = {
            "user_id": str(user_id),
            "source": source,
            "signal_type": signal_type,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "spotify_id": spotify_id,
            "strength": strength,
            "metadata": metadata or {},
            "import_batch_id": import_batch_id,
        }
        if occurred_at is not None:
            payload["occurred_at"] = occurred_at

        existing = self.client.select_one(
            self.table,
            user_id=str(user_id),
            source=source,
            signal_type=signal_type,
            entity_type=entity_type,
            entity_id=entity_id,
        )
        if existing:
            rows = self.client.update(self.table, {"id": existing["id"]}, payload)
            return rows[0] if rows else existing
        return self.client.insert_one(self.table, payload)

    def recent_for_user(self, user_id, limit=25):
        return self.client.select(self.table, user_id=str(user_id), order="occurred_at.desc", limit=limit)
