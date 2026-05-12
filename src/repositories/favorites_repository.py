class FavoritesRepository:
    def __init__(self, mongo):
        self.users = mongo.db.users

    def list_for_user(self, user):
        return user.get('favorites', []) if user else []

    def exists(self, user, entity_id):
        return any(fav.get('entityId') == entity_id for fav in self.list_for_user(user))

    def add_for_email(self, email, favorite):
        return self.users.update_one({'email': email}, {'$push': {'favorites': favorite}})

    def remove_for_email(self, email, entity_id):
        return self.users.update_one(
            {'email': email},
            {'$pull': {'favorites': {'entityId': entity_id}}}
        )
