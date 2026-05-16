class SupabaseMusicEntitiesRepository:
    table = "music_entities"

    def __init__(self, client):
        self.client = client

    def upsert(self, entity_type, spotify_id, name, image=None, artist=None, album=None, spotify_url=None, metadata=None):
        payload = {
            "entity_type": entity_type,
            "spotify_id": spotify_id,
            "name": name,
            "image": image,
            "artist": artist,
            "album": album,
            "spotify_url": spotify_url,
            "metadata": metadata or {},
        }
        existing = self.client.select_one(self.table, entity_type=entity_type, spotify_id=spotify_id)
        if existing:
            rows = self.client.update(self.table, {"entity_type": entity_type, "spotify_id": spotify_id}, payload)
            return rows[0] if rows else existing
        return self.client.insert_one(self.table, payload)
