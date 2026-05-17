import importlib
import os
import unittest

os.environ.setdefault("JWT_SECRET_KEY", "test-jwt-secret")
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-service-role-key")
os.environ.setdefault("CREATE_DB_INDEXES", "false")


class AuthSecurityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_module = importlib.import_module("src.app")
        cls.app_module.app.config["TESTING"] = True

    def test_spotify_state_round_trips_signed_payload(self):
        state = self.app_module.encode_spotify_state(
            "User@Example.com",
            "frontsb://login?next=spotify",
        )

        email, return_url = self.app_module.decode_spotify_state(state)

        self.assertEqual(email, "user@example.com")
        self.assertEqual(return_url, "frontsb://login?next=spotify")

    def test_spotify_state_rejects_tampering(self):
        state = self.app_module.encode_spotify_state("user@example.com")

        with self.assertRaises(ValueError):
            self.app_module.decode_spotify_state(state + "tampered")

    def test_spotify_state_rejects_expired_payload(self):
        state = self.app_module.encode_spotify_state("user@example.com")
        original_max_age = self.app_module.app.config["SPOTIFY_STATE_MAX_AGE_SECONDS"]
        self.app_module.app.config["SPOTIFY_STATE_MAX_AGE_SECONDS"] = -1
        try:
            with self.assertRaises(ValueError):
                self.app_module.decode_spotify_state(state)
        finally:
            self.app_module.app.config["SPOTIFY_STATE_MAX_AGE_SECONDS"] = original_max_age

    def test_spotify_state_drops_untrusted_return_url(self):
        state = self.app_module.encode_spotify_state(
            "user@example.com",
            "https://evil.example/callback",
        )

        _email, return_url = self.app_module.decode_spotify_state(state)

        self.assertIsNone(return_url)

    def test_spotify_exchange_code_is_short_lived(self):
        with self.app_module.app.app_context():
            code = self.app_module.create_spotify_exchange_code("user@example.com")
            payload = self.app_module.app.extensions["spotify_auth_codes"].get(code)

        self.assertEqual(payload, {"email": "user@example.com"})


if __name__ == "__main__":
    unittest.main()
