from flask import Blueprint, jsonify, request, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity
from marshmallow import ValidationError
import logging

from ..utils.api import get_json_body, get_string_field, get_int_arg, validate_entity_type, rate_limit, internal_error

logger = logging.getLogger(__name__)
bp = Blueprint("reviews", __name__)


def _get_repos():
    return current_app.extensions["repositories"]


def _serialize_review(review, repos=None):
    if not review:
        return None

    user_id = str(review.get("userId") or review.get("user_id") or "")
    user = repos.users.find_by_id(user_id) if repos and user_id else None
    return {
        "id": str(review.get("_id") or review.get("id")),
        "userId": user_id,
        "username": user.get("username") if user else None,
        "profilePicture": user.get("profile_picture") if user else None,
        "entityType": review.get("entityType"),
        "entityId": review.get("entityId"),
        "spotifyId": review.get("spotifyId"),
        "ratingId": review.get("ratingId"),
        "reviewText": review.get("reviewText"),
        "language": review.get("language"),
        "helpfulCount": review.get("helpfulCount", 0),
        "notHelpfulCount": review.get("notHelpfulCount", 0),
        "createdAt": str(review.get("createdAt") or ""),
        "updatedAt": str(review.get("updatedAt") or ""),
    }


@bp.route("/reviews", methods=["POST"])
@jwt_required()
@rate_limit(40)
def create_review():
    try:
        data = get_json_body()
        entity_type = get_string_field(data, "entityType", max_length=20)
        validate_entity_type(entity_type)
        entity_id = get_string_field(data, "entityId", max_length=120)
        review_text = get_string_field(data, "reviewText", max_length=4000)
        spotify_id = get_string_field(data, "spotifyId", required=False, max_length=120)
        rating_id = get_string_field(data, "ratingId", required=False, max_length=120)
        language = get_string_field(data, "language", required=False, max_length=12)

        repos = _get_repos()
        user = repos.users.find_by_email(get_jwt_identity())
        if not user:
            return jsonify({"message": "Usuario no encontrado."}), 404

        review = repos.reviews.create(user["_id"], entity_type, entity_id, review_text, spotify_id=spotify_id, rating_id=rating_id, language=language)
        repos.music_signals.create(
            user["_id"],
            "songbox",
            "review",
            entity_type,
            entity_id=entity_id,
            spotify_id=spotify_id,
            strength=min(1.0, max(0.35, len(review_text.strip()) / 800)),
            metadata={"reviewId": str(review.get("_id") or review.get("id")), "preview": review_text[:180]},
        )
        return jsonify({"review": _serialize_review(review, repos)}), 201

    except ValidationError:
        raise
    except Exception:
        logger.exception("Error al crear review")
        return internal_error("Error al crear la crítica.")


@bp.route("/reviews", methods=["GET"])
@jwt_required()
@rate_limit(80)
def list_reviews():
    try:
        entity_type = request.args.get("entityType", "").strip()
        entity_id = request.args.get("entityId", "").strip()
        limit = get_int_arg("limit", default=25, minimum=1, maximum=100)

        if not entity_type or not entity_id:
            return jsonify({"message": "entityType y entityId son obligatorios."}), 400
        validate_entity_type(entity_type)

        repos = _get_repos()
        reviews = repos.reviews.list_for_entity(entity_type, entity_id, limit=limit)
        return jsonify({"reviews": [_serialize_review(review, repos) for review in reviews]}), 200

    except ValidationError:
        raise
    except Exception:
        logger.exception("Error al listar reviews")
        return internal_error("Error al obtener las críticas.")


@bp.route("/profile/reviews", methods=["GET"])
@jwt_required()
@rate_limit(80)
def list_profile_reviews():
    repos = _get_repos()
    user = repos.users.find_by_email(get_jwt_identity())
    if not user:
        return jsonify({"message": "Usuario no encontrado."}), 404

    limit = get_int_arg("limit", default=10, minimum=1, maximum=50)
    reviews = repos.reviews.list_for_user(user["_id"], limit=limit)
    return jsonify({"reviews": [_serialize_review(review, repos) for review in reviews]}), 200
