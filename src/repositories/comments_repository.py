from bson import ObjectId


class CommentsRepository:
    def __init__(self, mongo):
        self.collection = mongo.db.comments

    def create(self, comment):
        result = self.collection.insert_one(comment)
        return self.find_by_id(result.inserted_id)

    def find_by_id(self, comment_id):
        return self.collection.find_one({'_id': ObjectId(comment_id)})

    def find_for_entity(self, comment_id, entity_type, entity_id):
        return self.collection.find_one({
            '_id': ObjectId(comment_id),
            'entity_type': entity_type,
            'entity_id': entity_id,
        })

    def delete_by_id(self, comment_id):
        return self.collection.delete_one({'_id': ObjectId(comment_id)})

    def list_for_entity(self, entity_type, entity_id, skip, limit):
        return self.collection.find(
            {'entity_type': entity_type, 'entity_id': entity_id}
        ).sort([('likes', -1), ('timestamp', -1)]).skip(skip).limit(limit)

    def count_for_entity(self, entity_type, entity_id):
        return self.collection.count_documents({
            'entity_type': entity_type,
            'entity_id': entity_id,
        })

    def update_reaction(self, comment_id, update_fields):
        return self.collection.update_one({'_id': ObjectId(comment_id)}, update_fields)
