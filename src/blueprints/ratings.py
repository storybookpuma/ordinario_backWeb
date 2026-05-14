from flask import Blueprint, jsonify, request, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity
from marshmallow import ValidationError
import logging

from ..utils.api import get_json_body, get_string_field, rate_limit, internal_error

logger = logging.getLogger(__name__)
bp = Blueprint("ratings", __name__)


def _get_repos():
    return current_app.extensions["repositories"]


def _invalidate_rating_cache(entity_type, entity_id):
    cache = current_app.extensions.get("cache")
    if cache:
        cache.delete(f"details:{entity_type}:{entity_id}")
        cache.delete(f"charts:top_rated:{entity_type}:10")
        cache.delete(f"charts:top_rated:{entity_type}:20")
        cache.delete(f"charts:top_rated:{entity_type}:50")
        cache.delete(f"activity:global:10")
        cache.delete(f"activity:global:20")
        cache.delete(f"activity:global:50")


def _validate_rating_payload(data):
    entity_type = get_string_field(data, "entityType", max_length=20)
    entity_id = get_string_field(data, "entityId", max_length=120)
    rating = data.get("rating")

    if entity_type not in ("song", "album", "artist"):
        raise ValidationError({"entityType": ["Tipo de entidad inválido."]})

    if not isinstance(rating, int) or not (1 <= rating <= 10):
        raise ValidationError({"rating": ["La calificación debe ser un número entre 1 y 10."]})

    return entity_type, entity_id, rating


@bp.route("/rate_entity", methods=["POST"])
@jwt_required()
@rate_limit(60)
def rate_entity():
    try:
        data = get_json_body()
        entity_type, entity_id, rating = _validate_rating_payload(data)

        logger.info(f"rate_entity: entity_type={entity_type}, entity_id={entity_id}, rating={rating}")

        repos = _get_repos()
        user_email = get_jwt_identity()
        user = repos.users.find_by_email(user_email)
        if not user:
            logger.warning(f"User not found: {user_email}")
            return jsonify({"message": "Usuario no encontrado."}), 404

        user_id = user["_id"]
        existing = repos.ratings.find_user_rating(entity_type, entity_id, user_id)
        if existing:
            logger.info(f"Usuario {user_email} ya ha calificado la entidad {entity_id}")
            return jsonify({"message": "Ya has calificado esta entidad."}), 400

        repos.ratings.create(entity_type, entity_id, user_id, rating)
        summary = repos.ratings.summarize_entity(entity_type, entity_id)
        _invalidate_rating_cache(entity_type, entity_id)
        logger.info(f"Entidad {entity_id} averageRating={summary['averageRating']}, ratingCount={summary['ratingCount']}")
        return jsonify({
            "message": "Calificación añadida correctamente.",
            "averageRating": summary["averageRating"],
            "ratingCount": summary["ratingCount"],
        }), 201

    except ValidationError:
        raise
    except Exception:
        logger.exception("Error al añadir calificación")
        return internal_error("Error al añadir calificación.")


@bp.route("/rate_entity", methods=["PUT"])
@jwt_required()
@rate_limit(60)
def update_rate_entity():
    try:
        data = get_json_body()
        entity_type, entity_id, rating = _validate_rating_payload(data)

        repos = _get_repos()
        user_email = get_jwt_identity()
        user = repos.users.find_by_email(user_email)
        if not user:
            return jsonify({"message": "Usuario no encontrado."}), 404

        user_id = user["_id"]
        existing = repos.ratings.find_user_rating(entity_type, entity_id, user_id)
        if not existing:
            return jsonify({"message": "Aún no has calificado esta entidad."}), 404

        repos.ratings.update_rating(entity_type, entity_id, user_id, rating)
        summary = repos.ratings.summarize_entity(entity_type, entity_id)
        _invalidate_rating_cache(entity_type, entity_id)
        return jsonify({
            "message": "Calificación actualizada correctamente.",
            "averageRating": summary["averageRating"],
            "ratingCount": summary["ratingCount"],
        }), 200

    except ValidationError:
        raise
    except Exception:
        logger.exception("Error al actualizar calificación")
        return internal_error("Error al actualizar calificación.")


@bp.route("/rate_entity", methods=["DELETE"])
@jwt_required()
@rate_limit(60)
def delete_rate_entity():
    try:
        data = get_json_body()
        entity_type = get_string_field(data, "entityType", max_length=20)
        entity_id = get_string_field(data, "entityId", max_length=120)

        if entity_type not in ("song", "album", "artist"):
            return jsonify({"message": "Tipo de entidad inválido."}), 400

        repos = _get_repos()
        user_email = get_jwt_identity()
        user = repos.users.find_by_email(user_email)
        if not user:
            return jsonify({"message": "Usuario no encontrado."}), 404

        user_id = user["_id"]
        existing = repos.ratings.find_user_rating(entity_type, entity_id, user_id)
        if not existing:
            return jsonify({"message": "Aún no has calificado esta entidad."}), 404

        repos.ratings.delete_rating(entity_type, entity_id, user_id)
        summary = repos.ratings.summarize_entity(entity_type, entity_id)
        _invalidate_rating_cache(entity_type, entity_id)
        return jsonify({
            "message": "Calificación eliminada correctamente.",
            "averageRating": summary["averageRating"],
            "ratingCount": summary["ratingCount"],
        }), 200

    except ValidationError:
        raise
    except Exception:
        logger.exception("Error al eliminar calificación")
        return internal_error("Error al eliminar calificación.")


@bp.route("/get_user_rating", methods=["GET"])
@jwt_required()
def get_user_rating():
    entity_type = request.args.get("entityType", "").strip()
    entity_id = request.args.get("entityId", "").strip()

    logger.info(f"get_user_rating: entity_type={entity_type}, entity_id={entity_id}")

    if not entity_type or not entity_id:
        logger.warning("Missing parameters in get_user_rating request.")
        return jsonify({"message": "Todos los parámetros son obligatorios."}), 400

    if entity_type not in ("song", "album", "artist"):
        logger.warning(f"Invalid entityType: {entity_type}")
        return jsonify({"message": "Tipo de entidad inválido."}), 400

    repos = _get_repos()
    user_email = get_jwt_identity()
    user = repos.users.find_by_email(user_email)
    if not user:
        logger.warning(f"User not found: {user_email}")
        return jsonify({"message": "Usuario no encontrado."}), 404

    user_id = user["_id"]
    existing = repos.ratings.find_user_rating(entity_type, entity_id, user_id)
    if existing:
        logger.info(f"Calificación encontrada: {existing['rating']}")
        return jsonify({"rating": existing["rating"]}), 200
    else:
        logger.info("No se encontró calificación existente.")
        return jsonify({"rating": 0}), 200
