class SupabaseRatingsRepository:
    table = "ratings"

    def __init__(self, client):
        self.client = client

    def ensure_indexes(self):
        return None

    def find_user_rating(self, entity_type, entity_id, user_id):
        return self._to_app_rating(self.client.select_one(
            self.table,
            entity_type=entity_type,
            entity_id=entity_id,
            user_id=str(user_id),
        ))

    def create(self, entity_type, entity_id, user_id, rating):
        return self.client.insert_one(self.table, {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "user_id": str(user_id),
            "rating": rating,
        })

    def update_rating(self, entity_type, entity_id, user_id, rating):
        return self.client.update(
            self.table,
            {"entity_type": entity_type, "entity_id": entity_id, "user_id": str(user_id)},
            {"rating": rating},
        )

    def delete_rating(self, entity_type, entity_id, user_id):
        return self.client.delete(
            self.table,
            entity_type=entity_type,
            entity_id=entity_id,
            user_id=str(user_id),
        )

    def summarize_entity(self, entity_type, entity_id):
        rows = self.client.select(self.table, entity_type=entity_type, entity_id=entity_id)
        if not rows:
            return {"averageRating": 0, "ratingCount": 0}

        ratings = [row["rating"] for row in rows]
        return {
            "averageRating": sum(ratings) / len(ratings),
            "ratingCount": len(ratings),
        }

    def top_rated(self, entity_type, limit=20):
        rows = self.client.select(self.table, entity_type=entity_type, limit=1000)
        grouped = {}
        for row in rows:
            eid = row["entity_id"]
            if eid not in grouped:
                grouped[eid] = {"ratings": [], "count": 0}
            grouped[eid]["ratings"].append(row["rating"])
            grouped[eid]["count"] += 1

        results = []
        for eid, data in grouped.items():
            avg = sum(data["ratings"]) / len(data["ratings"])
            results.append({
                "_id": eid,
                "averageRating": avg,
                "ratingCount": data["count"],
            })

        results.sort(key=lambda x: (-x["averageRating"], -x["ratingCount"]))
        return results[:limit]

    def _to_app_rating(self, rating):
        if not rating:
            return None

        return {
            "entityType": rating["entity_type"],
            "entityId": rating["entity_id"],
            "userId": rating["user_id"],
            "rating": rating["rating"],
        }
