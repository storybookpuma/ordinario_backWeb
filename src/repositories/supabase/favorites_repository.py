class SupabaseFavoritesRepository:
    table = "favorites"

    def __init__(self, client):
        self.client = client

    def list_for_user(self, user):
        if not user:
            return []

        rows = self.client.select(self.table, user_id=user["_id"], order="created_at.desc")
        return [self._to_app_favorite(row) for row in rows]

    def exists(self, user, entity_id):
        if not user:
            return False

        return self.client.select_one(self.table, user_id=user["_id"], entity_id=entity_id) is not None

    def add_for_email(self, email, favorite):
        user = self.client.select_one("app_users", email=email)
        if not user:
            return None

        return self.client.insert_one(self.table, {
            "user_id": user["id"],
            "entity_type": favorite["entityType"],
            "entity_id": favorite["entityId"],
            "name": favorite.get("name"),
            "image": favorite.get("image"),
            "artist": favorite.get("artist"),
        })

    def remove_for_email(self, email, entity_id):
        user = self.client.select_one("app_users", email=email)
        if not user:
            return None

        return self.client.delete(self.table, user_id=user["id"], entity_id=entity_id)

    def _to_app_favorite(self, favorite):
        return {
            "entityType": favorite["entity_type"],
            "entityId": favorite["entity_id"],
            "name": favorite.get("name"),
            "image": favorite.get("image"),
            "artist": favorite.get("artist"),
        }
