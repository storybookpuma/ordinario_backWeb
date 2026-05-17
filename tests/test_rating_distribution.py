import unittest

from src.repositories.supabase.ratings_repository import SupabaseRatingsRepository


class FakeSupabaseClient:
    def __init__(self, rows):
        self.rows = rows

    def select(self, _table, **_filters):
        return self.rows


class RatingDistributionTests(unittest.TestCase):
    def test_supabase_summary_returns_rating_distribution(self):
        repository = SupabaseRatingsRepository(FakeSupabaseClient([
            {"rating": 9},
            {"rating": 9},
            {"rating": 5},
        ]))

        summary = repository.summarize_entity("album", "album-1")

        self.assertEqual(summary["ratingCount"], 3)
        self.assertEqual(summary["ratingDistribution"]["9"], 2)
        self.assertEqual(summary["ratingDistribution"]["5"], 1)
        self.assertEqual(summary["ratingDistribution"]["10"], 0)

    def test_empty_summary_returns_zero_distribution(self):
        repository = SupabaseRatingsRepository(FakeSupabaseClient([]))

        summary = repository.summarize_entity("artist", "artist-1")

        self.assertEqual(summary["ratingCount"], 0)
        self.assertTrue(all(value == 0 for value in summary["ratingDistribution"].values()))


if __name__ == "__main__":
    unittest.main()
