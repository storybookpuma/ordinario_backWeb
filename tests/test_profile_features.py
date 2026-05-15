import importlib
import os
import unittest


os.environ.setdefault("JWT_SECRET_KEY", "test-jwt-secret")
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017/test")
os.environ.setdefault("CREATE_DB_INDEXES", "false")


class FakeCursor(list):
    def sort(self, *_args, **_kwargs):
        return self

    def limit(self, value):
        return FakeCursor(self[:value])


class FakeCollection:
    def __init__(self, rows):
        self.rows = rows

    def find(self, _query):
        return FakeCursor(self.rows)


class FakeRatingsRepository:
    def __init__(self, rows):
        self.collection = FakeCollection(rows)


class FakeUsersRepository:
    def __init__(self):
        self.users = {
            "me@example.com": {
                "_id": "me",
                "email": "me@example.com",
                "username": "Me",
                "favorites": [
                    {"entityType": "artist", "entityId": "a1", "name": "SZA"},
                    {"entityType": "album", "entityId": "al1", "name": "CTRL", "artist": "SZA"},
                ],
            },
            "target@example.com": {
                "_id": "target",
                "email": "target@example.com",
                "username": "Target",
                "favorites": [
                    {"entityType": "artist", "entityId": "a1", "name": "SZA"},
                    {"entityType": "song", "entityId": "s1", "name": "Good Days", "artist": "SZA"},
                ],
            },
        }

    def find_by_email(self, email):
        return self.users.get(email)

    def find_by_id(self, user_id):
        return next((user for user in self.users.values() if user["_id"] == user_id), None)


class FakeSpotify:
    def __init__(self, *_args, **_kwargs):
        pass

    def current_user_playing_track(self):
        return {
            "is_playing": True,
            "item": {
                "id": "track-1",
                "name": "Saturn",
                "artists": [{"name": "SZA"}],
                "album": {"name": "Lana", "images": [{"url": "https://example.com/cover.jpg"}]},
                "external_urls": {"spotify": "https://open.spotify.com/track/track-1"},
            },
        }


class ProfileFeatureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_module = importlib.import_module("src.app")
        cls.app_module.app.config["TESTING"] = True
        cls.app_module.app.config["DATABASE_PROVIDER"] = "mongo"

    def setUp(self):
        self.app_module.users_repository = FakeUsersRepository()
        self.app_module.favorites_repository = object()
        self.app_module.ratings_repository = FakeRatingsRepository([
            {
                "entityType": "song",
                "entityId": "s1",
                "userId": "me",
                "rating": 9,
                "name": "Good Days",
                "artist": "SZA",
                "timestamp": "2026-05-10T12:00:00+00:00",
            },
            {
                "entityType": "album",
                "entityId": "al1",
                "userId": "me",
                "rating": 8,
                "name": "CTRL",
                "artist": "SZA",
                "timestamp": "2026-05-11T12:00:00+00:00",
            },
        ])
        self.client = self.app_module.app.test_client()
        with self.app_module.app.app_context():
            self.token = self.app_module.create_access_token(identity="me@example.com")

    def auth_headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def test_monthly_wrapped_returns_personal_stats(self):
        response = self.client.get("/wrapped/monthly?month=2026-05", headers=self.auth_headers())

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["summary"]["ratingsCount"], 2)
        self.assertEqual(payload["summary"]["averageRating"], 8.5)
        self.assertEqual(payload["ratingsByType"]["song"], 1)

    def test_profile_compatibility_returns_shared_items(self):
        response = self.client.get("/profile_compatibility?profile_id=target", headers=self.auth_headers())

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertGreater(payload["score"], 0)
        self.assertEqual(payload["sharedCount"], 1)
        self.assertEqual(payload["sharedItems"][0]["name"], "SZA")

    def test_currently_playing_returns_track(self):
        self.app_module.get_valid_spotify_token = lambda *_args: "spotify-token"
        self.app_module.spotipy.Spotify = FakeSpotify

        response = self.client.get("/spotify/currently_playing", headers=self.auth_headers())

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["is_playing"])
        self.assertEqual(payload["item"]["name"], "Saturn")


if __name__ == "__main__":
    unittest.main()
