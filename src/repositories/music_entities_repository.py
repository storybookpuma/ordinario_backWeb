from datetime import datetime


class MusicEntitiesRepository:
    def __init__(self, mongo):
        self.collection = mongo.db.music_entities

    def upsert(self, entity_type, spotify_id, name, image=None, artist=None, album=None, spotify_url=None, metadata=None):
        payload = {
            "entityType": entity_type,
            "spotifyId": spotify_id,
            "name": name,
            "image": image,
            "artist": artist,
            "album": album,
            "spotifyUrl": spotify_url,
            "metadata": metadata or {},
            "updatedAt": datetime.utcnow(),
        }
        self.collection.update_one(
            {"entityType": entity_type, "spotifyId": spotify_id},
            {"$set": payload, "$setOnInsert": {"createdAt": datetime.utcnow()}},
            upsert=True,
        )
        return payload
