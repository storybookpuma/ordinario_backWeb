from datetime import datetime


class ReviewsRepository:
    def __init__(self, mongo):
        self.collection = mongo.db.reviews

    def ensure_indexes(self):
        self.collection.create_index([("entityType", 1), ("entityId", 1), ("createdAt", -1)])
        self.collection.create_index([("userId", 1), ("createdAt", -1)])

    def create(self, user_id, entity_type, entity_id, review_text, spotify_id=None, rating_id=None, language=None):
        doc = {
            "userId": str(user_id),
            "entityType": entity_type,
            "entityId": entity_id,
            "spotifyId": spotify_id,
            "ratingId": str(rating_id) if rating_id else None,
            "reviewText": review_text,
            "language": language,
            "helpfulCount": 0,
            "notHelpfulCount": 0,
            "createdAt": datetime.utcnow(),
            "updatedAt": datetime.utcnow(),
        }
        result = self.collection.insert_one(doc)
        doc["_id"] = result.inserted_id
        return doc

    def list_for_entity(self, entity_type, entity_id, limit=25):
        return list(self.collection.find({"entityType": entity_type, "entityId": entity_id}).sort("createdAt", -1).limit(limit))

    def list_for_user(self, user_id, limit=25):
        return list(self.collection.find({"userId": str(user_id)}).sort("createdAt", -1).limit(limit))
