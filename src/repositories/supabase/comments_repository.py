from datetime import datetime


class SupabaseCommentsRepository:
    comments_table = "comments"
    reactions_table = "comment_reactions"
    users_table = "app_users"

    def __init__(self, client):
        self.client = client

    def create(self, comment):
        row = self.client.insert_one(self.comments_table, {
            "entity_type": comment["entity_type"],
            "entity_id": str(comment["entity_id"]),
            "user_id": str(comment["user_id"]),
            "comment_text": comment["comment_text"],
            "name": comment.get("name"),
            "image": comment.get("image"),
            "artist": comment.get("artist"),
        })
        return self._to_app_comment(row)

    def find_by_id(self, comment_id):
        return self._to_app_comment(self.client.select_one(self.comments_table, id=str(comment_id)))

    def find_for_entity(self, comment_id, entity_type, entity_id):
        return self._to_app_comment(self.client.select_one(
            self.comments_table,
            id=str(comment_id),
            entity_type=entity_type,
            entity_id=str(entity_id),
        ))

    def delete_by_id(self, comment_id):
        return self.client.delete(self.comments_table, id=str(comment_id))

    def list_for_entity(self, entity_type, entity_id, skip, limit):
        rows = self.client.select(
            self.comments_table,
            entity_type=entity_type,
            entity_id=str(entity_id),
            limit=limit,
            offset=skip,
            order="created_at.desc",
        )
        comments = [self._to_app_comment(row) for row in rows]
        return sorted(
            comments,
            key=lambda item: (-item["likes"], -item["timestamp"].timestamp()),
        )

    def count_for_entity(self, entity_type, entity_id):
        return len(self.client.select(
            self.comments_table,
            entity_type=entity_type,
            entity_id=str(entity_id),
            columns="id",
        ))

    def update_reaction(self, comment_id, update_fields):
        reaction_field = update_fields.get("$addToSet", {})
        pull_field = update_fields.get("$pull", {})

        result = None

        if "liked_by" in pull_field:
            result = self._delete_reaction(comment_id, pull_field["liked_by"])
        if "disliked_by" in pull_field:
            result = self._delete_reaction(comment_id, pull_field["disliked_by"])
        if "liked_by" in reaction_field:
            return self._set_reaction(comment_id, reaction_field["liked_by"], "like")
        if "disliked_by" in reaction_field:
            return self._set_reaction(comment_id, reaction_field["disliked_by"], "dislike")
        if result is not None:
            return result

        raise NotImplementedError(f"Unsupported comment reaction update: {update_fields}")

    def _set_reaction(self, comment_id, user_id, reaction):
        self._delete_reaction(comment_id, user_id)
        return self.client.insert_one(self.reactions_table, {
            "comment_id": str(comment_id),
            "user_id": str(user_id),
            "reaction": reaction,
        })

    def _delete_reaction(self, comment_id, user_id):
        return self.client.delete(
            self.reactions_table,
            comment_id=str(comment_id),
            user_id=str(user_id),
        )

    def _to_app_comment(self, comment):
        if not comment:
            return None

        user = self.client.select_one(self.users_table, id=comment["user_id"])
        reactions = self.client.select(self.reactions_table, comment_id=comment["id"])
        liked_by = [reaction["user_id"] for reaction in reactions if reaction["reaction"] == "like"]
        disliked_by = [reaction["user_id"] for reaction in reactions if reaction["reaction"] == "dislike"]

        return {
            "_id": comment["id"],
            "entity_type": comment["entity_type"],
            "entity_id": comment["entity_id"],
            "user_id": comment["user_id"],
            "username": user.get("username", "") if user else "",
            "user_photo": user.get("profile_picture", "") if user else "",
            "user_email": user.get("email", "") if user else "",
            "comment_text": comment["comment_text"],
            "name": comment.get("name"),
            "image": comment.get("image"),
            "artist": comment.get("artist"),
            "timestamp": self._parse_timestamp(comment["created_at"]),
            "likes": len(liked_by),
            "dislikes": len(disliked_by),
            "liked_by": liked_by,
            "disliked_by": disliked_by,
        }

    def _parse_timestamp(self, value):
        if isinstance(value, datetime):
            return value

        return datetime.fromisoformat(value.replace("Z", "+00:00"))
