from datetime import datetime


class SpotifyProfilesRepository:
    def __init__(self, mongo):
        self.collection = mongo.db.spotify_profiles

    def upsert_for_user(self, user_id, profile):
        payload = {
            "userId": str(user_id),
            "spotifyUserId": profile.get("id"),
            "displayName": profile.get("display_name"),
            "email": profile.get("email"),
            "imageUrl": (profile.get("images") or [{}])[0].get("url"),
            "country": profile.get("country"),
            "product": profile.get("product"),
            "spotifyUrl": (profile.get("external_urls") or {}).get("spotify"),
            "syncedAt": datetime.utcnow(),
        }
        self.collection.update_one({"userId": str(user_id)}, {"$set": payload, "$setOnInsert": {"createdAt": datetime.utcnow()}}, upsert=True)
        return payload
