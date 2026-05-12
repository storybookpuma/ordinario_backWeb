import argparse
import json
import os
from pathlib import Path

import requests
from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient


load_dotenv()


class SupabaseRestClient:
    def __init__(self, url, service_role_key):
        self.base_url = f"{url.rstrip('/')}/rest/v1"
        self.headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json",
        }

    def select_one(self, table, **filters):
        params = {key: f"eq.{value}" for key, value in filters.items()}
        params["limit"] = "1"
        response = requests.get(
            f"{self.base_url}/{table}",
            headers=self.headers,
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        rows = response.json()
        return rows[0] if rows else None

    def insert_one(self, table, payload, dry_run=False):
        if dry_run:
            return {"id": f"dry-run-{table}-{payload.get('email') or payload.get('entity_id') or payload.get('comment_id') or 'row'}"}

        response = requests.post(
            f"{self.base_url}/{table}",
            headers={**self.headers, "Prefer": "return=representation"},
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        rows = response.json()
        return rows[0] if rows else None


def require_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def serialize_datetime(value):
    return value.isoformat() if value else None


def migrate_users(mongo_db, supabase, dry_run):
    id_map = {}
    users = list(mongo_db.users.find({}))

    for user in users:
        mongo_id = str(user["_id"])
        existing_user = None if dry_run else supabase.select_one("app_users", email=user["email"])

        if existing_user:
            id_map[mongo_id] = existing_user["id"]
            continue

        payload = {
            "username": user.get("username") or user["email"].split("@")[0],
            "email": user["email"],
            "password_hash": user["password"],
            "profile_picture": user.get("profile_picture", "/static/uploads/profile_pictures/default_picture.png"),
            "spotify_access_token": user.get("spotify_access_token"),
            "spotify_refresh_token": user.get("spotify_refresh_token"),
            "spotify_token_expires_at": user.get("spotify_token_expires_at"),
        }
        created_user = supabase.insert_one("app_users", payload, dry_run=dry_run)
        id_map[mongo_id] = created_user["id"]

    return users, id_map


def migrate_favorites(users, id_map, supabase, dry_run):
    count = 0
    for user in users:
        supabase_user_id = id_map[str(user["_id"])]
        for favorite in user.get("favorites", []):
            payload = {
                "user_id": supabase_user_id,
                "entity_type": favorite.get("entityType"),
                "entity_id": favorite.get("entityId"),
                "name": favorite.get("name"),
                "image": favorite.get("image"),
            }
            supabase.insert_one("favorites", payload, dry_run=dry_run)
            count += 1
    return count


def migrate_ratings(mongo_db, id_map, supabase, dry_run):
    count = 0
    for rating in mongo_db.rates.find({}):
        mongo_user_id = str(rating["userId"])
        if mongo_user_id not in id_map:
            continue

        payload = {
            "user_id": id_map[mongo_user_id],
            "entity_type": rating["entityType"],
            "entity_id": rating["entityId"],
            "rating": rating["rating"],
            "created_at": serialize_datetime(rating.get("timestamp")),
        }
        supabase.insert_one("ratings", payload, dry_run=dry_run)
        count += 1
    return count


def migrate_comments(mongo_db, id_map, supabase, dry_run):
    comment_id_map = {}
    comments_count = 0
    reactions_count = 0

    for comment in mongo_db.comments.find({}):
        mongo_user_id = str(comment["user_id"])
        if mongo_user_id not in id_map:
            continue

        payload = {
            "entity_type": comment["entity_type"],
            "entity_id": str(comment["entity_id"]),
            "user_id": id_map[mongo_user_id],
            "comment_text": comment["comment_text"],
            "created_at": serialize_datetime(comment.get("timestamp")),
        }
        created_comment = supabase.insert_one("comments", payload, dry_run=dry_run)
        supabase_comment_id = created_comment["id"]
        comment_id_map[str(comment["_id"])] = supabase_comment_id
        comments_count += 1

        for liked_user_id in comment.get("liked_by", []):
            liked_user_id = str(liked_user_id)
            if liked_user_id in id_map:
                supabase.insert_one("comment_reactions", {
                    "comment_id": supabase_comment_id,
                    "user_id": id_map[liked_user_id],
                    "reaction": "like",
                }, dry_run=dry_run)
                reactions_count += 1

        for disliked_user_id in comment.get("disliked_by", []):
            disliked_user_id = str(disliked_user_id)
            if disliked_user_id in id_map:
                supabase.insert_one("comment_reactions", {
                    "comment_id": supabase_comment_id,
                    "user_id": id_map[disliked_user_id],
                    "reaction": "dislike",
                }, dry_run=dry_run)
                reactions_count += 1

    return comments_count, reactions_count, comment_id_map


def migrate_follows(users, id_map, supabase, dry_run):
    count = 0
    seen = set()

    for user in users:
        follower_id = id_map[str(user["_id"])]
        for following_mongo_id in user.get("following", []):
            following_mongo_id = str(following_mongo_id)
            if following_mongo_id not in id_map:
                continue

            following_id = id_map[following_mongo_id]
            key = (follower_id, following_id)
            if key in seen or follower_id == following_id:
                continue

            supabase.insert_one("follows", {
                "follower_id": follower_id,
                "following_id": following_id,
            }, dry_run=dry_run)
            seen.add(key)
            count += 1

    return count


def write_id_map(output_path, id_map, comment_id_map):
    output = {
        "users": id_map,
        "comments": comment_id_map,
    }
    Path(output_path).write_text(json.dumps(output, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Migrate SongBox data from MongoDB to Supabase.")
    parser.add_argument("--dry-run", action="store_true", help="Count and validate records without writing to Supabase.")
    parser.add_argument("--output-id-map", default="migration_id_map.json", help="Where to write generated ID mappings.")
    args = parser.parse_args()

    mongo_uri = require_env("MONGO_URI")
    supabase_url = require_env("SUPABASE_URL")
    supabase_service_role_key = require_env("SUPABASE_SERVICE_ROLE_KEY")

    mongo_client = MongoClient(mongo_uri)
    mongo_db = mongo_client.get_default_database()
    supabase = SupabaseRestClient(supabase_url, supabase_service_role_key)

    users, id_map = migrate_users(mongo_db, supabase, args.dry_run)
    favorites_count = migrate_favorites(users, id_map, supabase, args.dry_run)
    ratings_count = migrate_ratings(mongo_db, id_map, supabase, args.dry_run)
    comments_count, reactions_count, comment_id_map = migrate_comments(mongo_db, id_map, supabase, args.dry_run)
    follows_count = migrate_follows(users, id_map, supabase, args.dry_run)

    write_id_map(args.output_id_map, id_map, comment_id_map)

    print("Migration summary")
    print(f"dry_run={args.dry_run}")
    print(f"users={len(users)}")
    print(f"favorites={favorites_count}")
    print(f"ratings={ratings_count}")
    print(f"comments={comments_count}")
    print(f"comment_reactions={reactions_count}")
    print(f"follows={follows_count}")
    print(f"id_map={args.output_id_map}")


if __name__ == "__main__":
    main()
