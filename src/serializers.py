from datetime import datetime


def serialize_current_user(user):
    user_id = user.get("_id") or user.get("id")
    return {
        "id": str(user_id) if user_id is not None else "",
        "username": user.get("username"),
        "email": user.get("email"),
        "profile_picture": user.get("profile_picture", ""),
        "favorites": user.get("favorites", []),
        "followers": user.get("followers", []),
        "following": user.get("following", []),
        "trivia_scores": user.get("trivia_scores", []),
        "spotify_connected": bool(user.get("spotify_refresh_token") or user.get("spotify_access_token")),
    }


def serialize_public_profile(user, *, include_favorites=False):
    user_id = user.get("_id") or user.get("id")
    profile = {
        "id": str(user_id) if user_id is not None else "",
        "username": user.get("username", ""),
        "profile_picture": user.get("profile_picture", ""),
    }
    if include_favorites:
        profile["favorites"] = user.get("favorites", [])
    return profile


def serialize_comment(comment, entity_type):
    comment["_id"] = str(comment["_id"])
    if entity_type == "profile":
        comment["entity_id"] = str(comment["entity_id"])
    comment["user_id"] = str(comment["user_id"])

    if isinstance(comment.get("timestamp"), datetime):
        comment["timestamp"] = comment["timestamp"].isoformat()
    else:
        try:
            comment["timestamp"] = datetime.fromisoformat(comment["timestamp"]).isoformat()
        except (ValueError, TypeError):
            comment["timestamp"] = "Desconocido"

    comment["likes"] = int(comment.get("likes", 0))
    comment["liked_by"] = [str(uid) for uid in comment.get("liked_by", [])] if isinstance(comment.get("liked_by"), list) else []
    if "dislikes" in comment:
        del comment["dislikes"]
    if "disliked_by" in comment:
        del comment["disliked_by"]
    return comment


def serialize_public_comment(comment, entity_type):
    serialized = serialize_comment(comment, entity_type)
    serialized.pop("user_email", None)
    return serialized
