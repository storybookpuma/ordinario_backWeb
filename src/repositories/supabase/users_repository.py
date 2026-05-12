class SupabaseUsersRepository:
    table = "app_users"
    follows_table = "follows"

    def __init__(self, client):
        self.client = client

    def find_by_email(self, email):
        return self._to_app_user(self.client.select_one(self.table, email=email))

    def find_by_username(self, username):
        return self._to_app_user(self.client.select_one(self.table, username=username))

    def find_by_id(self, user_id):
        return self._to_app_user(self.client.select_one(self.table, id=user_id))

    def get_profile_entity_id(self, user_id):
        return str(user_id) if self.find_by_id(user_id) else None

    def search_profiles(self, query, limit):
        # PostgREST full ilike/or filtering will be added before enabling Supabase provider.
        users = self.client.select(self.table, limit=limit)
        normalized_query = query.lower()
        return [
            self._to_app_user(user)
            for user in users
            if normalized_query in user.get("username", "").lower()
            or normalized_query in user.get("email", "").lower()
        ]

    def create(self, user):
        payload = {
            "username": user["username"],
            "email": user["email"],
            "password_hash": user["password"],
            "profile_picture": user.get("profile_picture", "/static/uploads/profile_pictures/default_picture.png"),
        }
        created_user = self.client.insert_one(self.table, payload)
        return InsertOneResult(created_user["id"] if created_user else None)

    def update_by_email(self, email, update):
        payload = self._mongo_update_to_payload(update)
        return self.client.update(self.table, {"email": email}, payload)

    def update_by_id(self, user_id, update):
        payload = self._mongo_update_to_payload(update)
        return self.client.update(self.table, {"id": str(user_id)}, payload)

    def add_to_set_by_id(self, user_id, field, value):
        if field == "following":
            return self._follow(user_id, value)
        if field == "followers":
            return self._follow(value, user_id)

        raise NotImplementedError(f"Unsupported add_to_set field for Supabase users: {field}")

    def pull_by_id(self, user_id, field, value):
        if field == "following":
            return self._unfollow(user_id, value)
        if field == "followers":
            return self._unfollow(value, user_id)

        raise NotImplementedError(f"Unsupported pull field for Supabase users: {field}")

    def find_many_by_ids(self, user_ids):
        # This method will move to a follows repository during the full Supabase migration.
        users = []
        for user_id in user_ids:
            user = self.find_by_id(user_id)
            if user:
                users.append(user)
        return users

    def _to_app_user(self, user):
        if not user:
            return None

        user_id = user["id"]

        return {
            "_id": user["id"],
            "username": user.get("username"),
            "email": user.get("email"),
            "password": user.get("password_hash"),
            "profile_picture": user.get("profile_picture", ""),
            "favorites": [],
            "followers": self._followers_for_user(user_id),
            "following": self._following_for_user(user_id),
            "spotify_access_token": user.get("spotify_access_token"),
            "spotify_refresh_token": user.get("spotify_refresh_token"),
            "spotify_token_expires_at": user.get("spotify_token_expires_at"),
        }

    def _follow(self, follower_id, following_id):
        existing_follow = self.client.select_one(
            self.follows_table,
            follower_id=str(follower_id),
            following_id=str(following_id),
        )
        if existing_follow:
            return existing_follow

        return self.client.insert_one(self.follows_table, {
            "follower_id": str(follower_id),
            "following_id": str(following_id),
        })

    def _unfollow(self, follower_id, following_id):
        return self.client.delete(
            self.follows_table,
            follower_id=str(follower_id),
            following_id=str(following_id),
        )

    def _followers_for_user(self, user_id):
        rows = self.client.select(self.follows_table, following_id=str(user_id), columns="follower_id")
        return [row["follower_id"] for row in rows]

    def _following_for_user(self, user_id):
        rows = self.client.select(self.follows_table, follower_id=str(user_id), columns="following_id")
        return [row["following_id"] for row in rows]

    def _mongo_update_to_payload(self, update):
        if "$set" not in update:
            raise NotImplementedError("Only $set updates are supported by SupabaseUsersRepository.")

        payload = dict(update["$set"])
        if "password" in payload:
            payload["password_hash"] = payload.pop("password")
        return payload


class InsertOneResult:
    def __init__(self, inserted_id):
        self.inserted_id = inserted_id
