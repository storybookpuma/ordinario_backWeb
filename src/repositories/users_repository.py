from bson import ObjectId
from bson.errors import InvalidId


class UsersRepository:
    def __init__(self, mongo):
        self.collection = mongo.db.users

    def find_by_email(self, email):
        return self.collection.find_one({'email': email})

    def find_by_username(self, username):
        return self.collection.find_one({'username': username})

    def search_profiles(self, query, limit, offset=0):
        regex_query = {'$regex': query, '$options': 'i'}
        return self.collection.find({
            'username': regex_query,
        }).skip(offset).limit(limit)

    def find_by_id(self, user_id):
        try:
            return self.collection.find_one({'_id': ObjectId(user_id)})
        except InvalidId:
            return None

    def get_profile_entity_id(self, user_id):
        try:
            entity_id = ObjectId(user_id)
        except InvalidId:
            return None

        return entity_id if self.collection.find_one({'_id': entity_id}) else None

    def create(self, user):
        return self.collection.insert_one(user)

    def update_by_email(self, email, update):
        return self.collection.update_one({'email': email}, update)

    def update_by_id(self, user_id, update):
        return self.collection.update_one({'_id': user_id}, update)

    def add_to_set_by_id(self, user_id, field, value):
        return self.collection.update_one({'_id': user_id}, {'$addToSet': {field: value}})

    def pull_by_id(self, user_id, field, value):
        return self.collection.update_one({'_id': user_id}, {'$pull': {field: value}})

    def find_many_by_ids(self, user_ids):
        object_ids = []
        for user_id in user_ids:
            try:
                object_ids.append(ObjectId(user_id))
            except InvalidId:
                continue

        if not object_ids:
            return []

        return list(self.collection.find({'_id': {'$in': object_ids}}))
