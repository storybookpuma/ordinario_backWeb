class SupabaseReviewsRepository:
    table = "reviews"

    def __init__(self, client):
        self.client = client

    def ensure_indexes(self):
        return None

    def create(self, user_id, entity_type, entity_id, review_text, spotify_id=None, rating_id=None, language=None):
        return self._to_app_review(self.client.insert_one(self.table, {
            "user_id": str(user_id),
            "entity_type": entity_type,
            "entity_id": entity_id,
            "spotify_id": spotify_id,
            "rating_id": str(rating_id) if rating_id else None,
            "review_text": review_text,
            "language": language,
        }))

    def list_for_entity(self, entity_type, entity_id, limit=25):
        rows = self.client.select(self.table, entity_type=entity_type, entity_id=entity_id, order="created_at.desc", limit=limit)
        return [self._to_app_review(row) for row in rows]

    def list_for_user(self, user_id, limit=25):
        rows = self.client.select(self.table, user_id=str(user_id), order="created_at.desc", limit=limit)
        return [self._to_app_review(row) for row in rows]

    def _to_app_review(self, review):
        if not review:
            return None
        return {
            "_id": review["id"],
            "userId": review["user_id"],
            "entityType": review["entity_type"],
            "entityId": review["entity_id"],
            "spotifyId": review.get("spotify_id"),
            "ratingId": review.get("rating_id"),
            "reviewText": review.get("review_text"),
            "language": review.get("language"),
            "helpfulCount": review.get("helpful_count", 0),
            "notHelpfulCount": review.get("not_helpful_count", 0),
            "createdAt": review.get("created_at"),
            "updatedAt": review.get("updated_at"),
        }
