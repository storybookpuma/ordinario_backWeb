from flask import Blueprint, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from marshmallow import ValidationError
import logging

from ..utils.api import get_json_body, get_string_field, validate_entity_type, rate_limit, internal_error

logger = logging.getLogger(__name__)
bp = Blueprint("favorites", __name__)


def _get_repos():
    from flask import current_app
    return current_app.extensions["repositories"]


def _record_favorite_signal(repos, user_id, favorite):
    try:
        repos.music_signals.create(
            user_id,
            "songbox",
            "favorite",
            favorite["entityType"],
            entity_id=favorite["entityId"],
            spotify_id=favorite["entityId"],
            strength=0.85,
            metadata={
                "name": favorite.get("name"),
                "image": favorite.get("image"),
                "artist": favorite.get("artist"),
            },
        )
    except Exception:
        logger.exception("Failed to record favorite music signal")


@bp.route("/add_favorite", methods=["POST"])
@jwt_required()
@rate_limit(80)
def add_favorite():
    try:
        data = get_json_body()
        entity_type = get_string_field(data, "entityType", max_length=20)
        validate_entity_type(entity_type)
        entity_id = get_string_field(data, "entityId", max_length=120)
        name = get_string_field(data, "name", required=False, max_length=200)
        image = get_string_field(data, "image", required=False, max_length=1000)
        artist = get_string_field(data, "artist", required=False, max_length=200)

        repos = _get_repos()
        current_user_email = get_jwt_identity()
        user = repos.users.find_by_email(current_user_email)
        if not user:
            return jsonify({"message": "Usuario no encontrado."}), 404

        if repos.favorites.exists(user, entity_id):
            return jsonify({"message": "El favorito ya existe."}), 400

        new_favorite = {
            "entityType": entity_type,
            "entityId": entity_id,
            "name": name,
            "image": image,
            "artist": artist,
        }
        repos.favorites.add_for_email(current_user_email, new_favorite)
        _record_favorite_signal(repos, user["_id"], new_favorite)
        return jsonify({"message": "Favorito agregado exitosamente."}), 200

    except ValidationError:
        raise
    except Exception:
        logger.exception("Error al agregar favorito")
        return internal_error("Error al agregar favorito.")


@bp.route("/remove_favorite", methods=["POST"])
@jwt_required()
@rate_limit(80)
def remove_favorite():
    try:
        data = get_json_body()
        entity_id = get_string_field(data, "entityId", max_length=120)

        repos = _get_repos()
        current_user_email = get_jwt_identity()
        repos.favorites.remove_for_email(current_user_email, entity_id)
        return jsonify({"message": "Favorito eliminado exitosamente."}), 200

    except ValidationError:
        raise
    except Exception:
        logger.exception("Error al eliminar favorito")
        return internal_error("Error al eliminar favorito.")


@bp.route("/get_favorites", methods=["GET"])
@jwt_required()
def get_favorites():
    repos = _get_repos()
    current_user_email = get_jwt_identity()
    user = repos.users.find_by_email(current_user_email)
    if not user:
        return jsonify({"message": "Usuario no encontrado."}), 404

    favorites = repos.favorites.list_for_user(user)
    return jsonify({"favorites": favorites}), 200
