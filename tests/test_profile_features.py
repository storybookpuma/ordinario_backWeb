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


class FakeFavoritesRepository:
    def list_for_user(self, user):
        return user.get("favorites", []) if user else []


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
                "type": "track",
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
        self.app_module.favorites_repository = FakeFavoritesRepository()
        self.app_module.ratings_repository = FakeRatingsRepository([
            {"entityType": "song", "entityId": "s1", "userId": "me", "rating": 9, "name": "Good Days", "artist": "SZA", "timestamp": "2026-05-10T12:00:00+00:00"},
            {"entityType": "album", "entityId": "al1", "userId": "me", "rating": 8, "name": "CTRL", "artist": "SZA", "timestamp": "2026-05-11T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s2", "userId": "me", "rating": 9, "name": "Kill Bill", "artist": "SZA", "timestamp": "2026-04-12T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s3", "userId": "me", "rating": 9, "name": "Saturn", "artist": "SZA", "timestamp": "2026-03-13T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s4", "userId": "me", "rating": 9, "name": "Snooze", "artist": "SZA", "timestamp": "2026-05-14T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s5", "userId": "me", "rating": 9, "name": "Broken Clocks", "artist": "SZA", "timestamp": "2026-05-15T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s6", "userId": "me", "rating": 9, "name": "Drew Barrymore", "artist": "SZA", "timestamp": "2026-05-16T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s7", "userId": "me", "rating": 9, "name": "The Weekend", "artist": "SZA", "timestamp": "2026-05-17T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s8", "userId": "me", "rating": 9, "name": "Supermodel", "artist": "SZA", "timestamp": "2026-05-18T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s9", "userId": "me", "rating": 9, "name": "Garden", "artist": "SZA", "timestamp": "2026-05-19T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s10", "userId": "me", "rating": 9, "name": "20 Something", "artist": "SZA", "timestamp": "2026-05-20T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s11", "userId": "me", "rating": 9, "name": "Normal Girl", "artist": "SZA", "timestamp": "2026-05-21T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s12", "userId": "me", "rating": 9, "name": "Anything", "artist": "SZA", "timestamp": "2026-05-22T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s13", "userId": "me", "rating": 9, "name": "Love Galore", "artist": "SZA", "timestamp": "2026-05-23T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s14", "userId": "me", "rating": 9, "name": "Doves", "artist": "SZA", "timestamp": "2026-05-24T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s15", "userId": "me", "rating": 9, "name": "Smoking", "artist": "SZA", "timestamp": "2026-05-25T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s16", "userId": "me", "rating": 9, "name": "Forgiveless", "artist": "SZA", "timestamp": "2026-05-26T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s17", "userId": "me", "rating": 9, "name": "Notice Me", "artist": "SZA", "timestamp": "2026-05-27T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s18", "userId": "me", "rating": 9, "name": "Gone Girl", "artist": "SZA", "timestamp": "2026-05-28T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s19", "userId": "me", "rating": 9, "name": "Blind", "artist": "SZA", "timestamp": "2026-05-29T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s20", "userId": "me", "rating": 9, "name": "Used", "artist": "SZA", "timestamp": "2026-05-30T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s21", "userId": "me", "rating": 9, "name": "Seek", "artist": "Frank Ocean", "timestamp": "2026-05-31T12:00:00+00:00"},
            {"entityType": "song", "entityId": "s22", "userId": "me", "rating": 9, "name": "Pink", "artist": "Tyler, The Creator", "timestamp": "2026-05-31T12:00:00+00:00"},
            {"entityType": "album", "entityId": "al2", "userId": "me", "rating": 8, "name": "SOS", "artist": "SZA", "timestamp": "2026-05-21T12:00:00+00:00"},
            {"entityType": "album", "entityId": "al3", "userId": "me", "rating": 8, "name": "Lana", "artist": "SZA", "timestamp": "2026-05-22T12:00:00+00:00"},
            {"entityType": "album", "entityId": "al4", "userId": "me", "rating": 8, "name": "Z", "artist": "SZA", "timestamp": "2026-05-23T12:00:00+00:00"},
            {"entityType": "album", "entityId": "al5", "userId": "me", "rating": 8, "name": "E", "artist": "SZA", "timestamp": "2026-05-24T12:00:00+00:00"},
            {"entityType": "album", "entityId": "al6", "userId": "me", "rating": 8, "name": "S", "artist": "SZA", "timestamp": "2026-05-25T12:00:00+00:00"},
            {"entityType": "album", "entityId": "al7", "userId": "me", "rating": 8, "name": "A", "artist": "SZA", "timestamp": "2026-05-26T12:00:00+00:00"},
            {"entityType": "album", "entityId": "al8", "userId": "me", "rating": 8, "name": "B", "artist": "SZA", "timestamp": "2026-05-27T12:00:00+00:00"},
            {"entityType": "album", "entityId": "al9", "userId": "me", "rating": 8, "name": "C", "artist": "SZA", "timestamp": "2026-05-28T12:00:00+00:00"},
            {"entityType": "album", "entityId": "al10", "userId": "me", "rating": 8, "name": "D", "artist": "SZA", "timestamp": "2026-05-29T12:00:00+00:00"},
            {"entityType": "album", "entityId": "al11", "userId": "me", "rating": 8, "name": "F", "artist": "SZA", "timestamp": "2026-05-30T12:00:00+00:00"},
            {"entityType": "album", "entityId": "al12", "userId": "me", "rating": 8, "name": "Blonde", "artist": "Frank Ocean", "timestamp": "2026-05-31T12:00:00+00:00"},
            {"entityType": "album", "entityId": "al13", "userId": "me", "rating": 8, "name": "Igor", "artist": "Tyler, The Creator", "timestamp": "2026-05-31T12:00:00+00:00"},
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
        self.assertGreaterEqual(payload["summary"]["ratingsCount"], 2)
        self.assertGreater(payload["summary"]["averageRating"], 8.0)
        self.assertIn("song", payload["ratingsByType"])

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

    def test_badges_returns_unlocked_badges(self):
        response = self.client.get("/badges", headers=self.auth_headers())

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        badge_ids = {b["id"] for b in payload["badges"]}
        self.assertIn("album_hunter", badge_ids)
        self.assertIn("song_critic", badge_ids)
        self.assertIn("generous_rater", badge_ids)
        self.assertIn("consistent", badge_ids)


if __name__ == "__main__":
    unittest.main()
