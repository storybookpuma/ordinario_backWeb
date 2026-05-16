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

    def create(self, entity_type, entity_id, user_id, rating, name=None, image=None, artist=None):
        doc = {
            'entityType': entity_type,
            'entityId': entity_id,
            'userId': user_id,
            'rating': rating,
            'timestamp': datetime.utcnow(),
        }
        if name is not None:
            doc['name'] = name
        if image is not None:
            doc['image'] = image
        if artist is not None:
            doc['artist'] = artist
        return self.collection.insert_one(doc)

    def update_rating(self, entity_type, entity_id, user_id, rating, name=None, image=None, artist=None):
        update = {'$set': {'rating': rating, 'timestamp': datetime.utcnow()}}
        if name is not None:
            update['$set']['name'] = name
        if image is not None:
            update['$set']['image'] = image
        if artist is not None:
            update['$set']['artist'] = artist
        return self.collection.update_one(
            {'entityType': entity_type, 'entityId': entity_id, 'userId': user_id},
            update
        )

    def delete_rating(self, entity_type, entity_id, user_id):
        return self.collection.delete_one({
            'entityType': entity_type,
            'entityId': entity_id,
            'userId': user_id,
        })

    def summarize_entity(self, entity_type, entity_id):
        pipeline = [
            {'$match': {'entityType': entity_type, 'entityId': entity_id}},
            {'$group': {
                '_id': None,
                'averageRating': {'$avg': '$rating'},
                'ratingCount': {'$sum': 1},
                'ratings': {'$push': '$rating'},
            }},
        ]
        result = list(self.collection.aggregate(pipeline))
        if not result:
            return {'averageRating': 0, 'ratingCount': 0, 'ratingDistribution': {str(value): 0 for value in range(1, 11)}}

        distribution = {str(value): 0 for value in range(1, 11)}
        for rating in result[0].get('ratings', []):
            key = str(int(rating))
            if key in distribution:
                distribution[key] += 1

        return {
            'averageRating': result[0]['averageRating'],
            'ratingCount': result[0]['ratingCount'],
            'ratingDistribution': distribution,
        }

    def top_rated(self, entity_type, limit=20):
        pipeline = [
            {'$match': {'entityType': entity_type}},
            {'$group': {
                '_id': '$entityId',
                'averageRating': {'$avg': '$rating'},
                'ratingCount': {'$sum': 1},
                'name': {'$first': '$name'},
                'image': {'$first': '$image'},
                'artist': {'$first': '$artist'},
            }},
            {'$match': {'ratingCount': {'$gte': 1}}},
            {'$sort': {'averageRating': -1, 'ratingCount': -1}},
            {'$limit': limit},
        ]
        return list(self.collection.aggregate(pipeline))
