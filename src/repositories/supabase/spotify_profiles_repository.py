class SupabaseSpotifyProfilesRepository:
    table = "spotify_profiles"

    def __init__(self, client):
        self.client = client

    def upsert_for_user(self, user_id, profile):
        image_url = None
        images = profile.get("images") or []
        if images:
            image_url = images[0].get("url")

        payload = {
            "user_id": str(user_id),
            "spotify_user_id": profile.get("id"),
            "display_name": profile.get("display_name"),
            "email": profile.get("email"),
            "image_url": image_url,
            "country": profile.get("country"),
            "product": profile.get("product"),
            "spotify_url": (profile.get("external_urls") or {}).get("spotify"),
        }

        existing = self.client.select_one(self.table, user_id=str(user_id))
        if existing:
            rows = self.client.update(self.table, {"user_id": str(user_id)}, payload)
            return rows[0] if rows else existing
        return self.client.insert_one(self.table, payload)
