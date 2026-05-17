import importlib
import os
from types import SimpleNamespace
import unittest


os.environ.setdefault("JWT_SECRET_KEY", "test-jwt-secret")
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-service-role-key")
os.environ.setdefault("CREATE_DB_INDEXES", "false")


class FakeSupabaseClient:
    def __init__(self, rows):
        self.rows = rows

    def select(self, table, **filters):
        rows = self.rows.get(table, [])
        user_id = filters.get("user_id")
        if user_id:
            rows = [row for row in rows if row.get("user_id") == user_id]
        limit = filters.get("limit")
        return rows[:limit] if limit else rows


class FakeRatingsRepository:
    def __init__(self, rows):
        self.client = FakeSupabaseClient({"ratings": rows})


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

    def setUp(self):
        self.app_module.users_repository = FakeUsersRepository()
        self.app_module.favorites_repository = FakeFavoritesRepository()
        rating_rows = [
            {"entity_type": "song", "entity_id": "s1", "user_id": "me", "rating": 9, "name": "Good Days", "artist": "SZA", "created_at": "2026-05-10T12:00:00+00:00"},
            {"entity_type": "album", "entity_id": "al1", "user_id": "me", "rating": 8, "name": "CTRL", "artist": "SZA", "created_at": "2026-05-11T12:00:00+00:00"},
            {"entity_type": "song", "entity_id": "s2", "user_id": "me", "rating": 9, "name": "Kill Bill", "artist": "SZA", "created_at": "2026-04-12T12:00:00+00:00"},
            {"entity_type": "song", "entity_id": "s3", "user_id": "me", "rating": 9, "name": "Saturn", "artist": "SZA", "created_at": "2026-03-13T12:00:00+00:00"},
            *[
                {"entity_type": "song", "entity_id": f"s{index}", "user_id": "me", "rating": 9, "name": f"Song {index}", "artist": "SZA", "created_at": f"2026-05-{index:02d}T12:00:00+00:00"}
                for index in range(4, 22)
            ],
            *[
                {"entity_type": "album", "entity_id": f"al{index}", "user_id": "me", "rating": 8, "name": f"Album {index}", "artist": "SZA", "created_at": f"2026-05-{index + 10:02d}T12:00:00+00:00"}
                for index in range(2, 12)
            ],
            {"entity_type": "album", "entity_id": "al12", "user_id": "me", "rating": 8, "name": "Blonde", "artist": "Frank Ocean", "created_at": "2026-05-31T12:00:00+00:00"},
            {"entity_type": "album", "entity_id": "al13", "user_id": "me", "rating": 8, "name": "Igor", "artist": "Tyler, The Creator", "created_at": "2026-05-31T12:00:00+00:00"},
        ]
        fake_client = FakeSupabaseClient({"ratings": rating_rows, "favorites": []})
        self.app_module.ratings_repository = FakeRatingsRepository(rating_rows)
        self.app_module.repositories = SimpleNamespace(
            ratings=SimpleNamespace(client=fake_client),
            favorites=SimpleNamespace(client=fake_client),
            comments=SimpleNamespace(client=fake_client),
        )
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
