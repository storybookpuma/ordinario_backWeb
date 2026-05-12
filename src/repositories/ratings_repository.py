from datetime import datetime


class RatingsRepository:
    def __init__(self, mongo):
        self.collection = mongo.db.rates

    def ensure_indexes(self):
        self.collection.create_index(
            [("entityType", 1), ("entityId", 1), ("userId", 1)],
            unique=True,
        )

    def find_user_rating(self, entity_type, entity_id, user_id):
        return self.collection.find_one({
            'entityType': entity_type,
            'entityId': entity_id,
            'userId': user_id,
        })

    def create(self, entity_type, entity_id, user_id, rating):
        return self.collection.insert_one({
            'entityType': entity_type,
            'entityId': entity_id,
            'userId': user_id,
            'rating': rating,
            'timestamp': datetime.utcnow(),
        })

    def summarize_entity(self, entity_type, entity_id):
        pipeline = [
            {'$match': {'entityType': entity_type, 'entityId': entity_id}},
            {'$group': {
                '_id': None,
                'averageRating': {'$avg': '$rating'},
                'ratingCount': {'$sum': 1},
            }},
        ]
        result = list(self.collection.aggregate(pipeline))
        if not result:
            return {'averageRating': 0, 'ratingCount': 0}

        return {
            'averageRating': result[0]['averageRating'],
            'ratingCount': result[0]['ratingCount'],
        }
