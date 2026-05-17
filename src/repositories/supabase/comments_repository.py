from datetime import datetime


class SupabaseCommentsRepository:
    comments_table = "comments"
    reactions_table = "comment_reactions"
    users_table = "app_users"

    def __init__(self, client):
        self.client = client

    def create(self, comment):
        payload = {
            "entity_type": comment["entity_type"],
            "entity_id": str(comment["entity_id"]),
            "user_id": str(comment["user_id"]),
            "comment_text": comment["comment_text"],
            "name": comment.get("name"),
            "image": comment.get("image"),
            "artist": comment.get("artist"),
        }
        if comment.get("parent_id"):
            payload["parent_id"] = comment["parent_id"]
        row = self.client.insert_one(self.comments_table, payload)
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
            parent_id="is.null",
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
            parent_id="is.null",
            columns="id",
        ))

    def list_replies(self, parent_id):
        rows = self.client.select(
            self.comments_table,
            parent_id=str(parent_id),
            order="created_at.asc",
        )
        return [self._to_app_comment(row) for row in rows]

    def reply_counts(self, comment_ids):
        result = {}
        for cid in comment_ids:
            rows = self.client.select(
                self.comments_table,
                parent_id=str(cid),
                columns="id",
            )
            result[str(cid)] = len(rows)
        return result

    def update_reaction(self, comment_id, update_fields):
        add_fields = update_fields.get("$addToSet", {})
        pull_fields = update_fields.get("$pull", {})

        if add_fields.get("liked_by"):
            return self._set_reaction(comment_id, add_fields["liked_by"], "like")
        if pull_fields.get("liked_by"):
            return self._delete_reaction(comment_id, pull_fields["liked_by"])

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
            "liked_by": liked_by,
        }

    def _parse_timestamp(self, value):
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
