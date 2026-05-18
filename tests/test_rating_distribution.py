import unittest

from src.repositories.supabase.ratings_repository import SupabaseRatingsRepository


class FakeSupabaseClient:
    def __init__(self, summary):
        self.summary = summary

    def rpc(self, _function_name, _payload):
        return self.summary


class RatingDistributionTests(unittest.TestCase):
    def test_supabase_summary_returns_rating_distribution(self):
        repository = SupabaseRatingsRepository(FakeSupabaseClient({
            "averageRating": 7.6666666667,
            "ratingCount": 3,
            "ratingDistribution": {"9": 2, "5": 1},
        }))

        summary = repository.summarize_entity("album", "album-1")

        self.assertEqual(summary["ratingCount"], 3)
        self.assertEqual(summary["ratingDistribution"]["9"], 2)
        self.assertEqual(summary["ratingDistribution"]["5"], 1)
        self.assertEqual(summary["ratingDistribution"]["10"], 0)

    def test_empty_summary_returns_zero_distribution(self):
        repository = SupabaseRatingsRepository(FakeSupabaseClient({
            "averageRating": 0,
            "ratingCount": 0,
            "ratingDistribution": {str(value): 0 for value in range(1, 11)},
        }))

        summary = repository.summarize_entity("artist", "artist-1")

        self.assertEqual(summary["ratingCount"], 0)
        self.assertTrue(all(value == 0 for value in summary["ratingDistribution"].values()))


if __name__ == "__main__":
    unittest.main()
