from datetime import datetime, timezone
from flask import Blueprint, jsonify, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity
from marshmallow import ValidationError
import logging

from ..utils.api import (
    get_json_body,
    get_string_field,
    get_int_arg,
    validate_entity_type,
    rate_limit,
    internal_error,
)
from ..serializers import serialize_public_comment

logger = logging.getLogger(__name__)

bp = Blueprint("comments", __name__, url_prefix="/<entity_type>/<entity_id>")


def _get_repos():
    return current_app.extensions["repositories"]


def _resolve_entity_read(entity_type, entity_id):
    repos = _get_repos()
    if entity_type == "profile":
        entity_obj_id = repos.users.get_profile_entity_id(entity_id)
        if not entity_obj_id:
            return None
        return entity_obj_id
    return entity_id


@bp.route("/comments", methods=["POST"])
@jwt_required()
@rate_limit(30)
def add_comment(entity_type, entity_id):
    try:
        validate_entity_type(entity_type)
        repos = _get_repos()

        current_user_email = get_jwt_identity()
        user = repos.users.find_by_email(current_user_email)
        if not user:
            return jsonify({"message": "Usuario no encontrado."}), 404

        user_id = str(user["_id"])
        username = user["username"]
        user_photo = user.get("profile_picture", "")

        data = get_json_body()
        comment_text = get_string_field(data, "comment_text", max_length=500)
        name = get_string_field(data, "name", required=False, max_length=200)
        image = get_string_field(data, "image", required=False, max_length=1000)
        artist = get_string_field(data, "artist", required=False, max_length=200)

        entity_obj_id = _resolve_entity_read(entity_type, entity_id)
        if entity_obj_id is None:
            return jsonify({"message": "ID de entidad inválido."}), 400

        comment = {
            "entity_type": entity_type,
            "entity_id": entity_obj_id,
            "user_id": user_id,
            "username": username,
            "user_photo": user_photo,
            "comment_text": comment_text,
            "name": name,
            "image": image,
            "artist": artist,
            "timestamp": datetime.now(timezone.utc),
            "likes": 0,
            "dislikes": 0,
            "liked_by": [],
            "disliked_by": [],
        }

        inserted = repos.comments.create(comment)
        inserted = serialize_public_comment(inserted, entity_type)
        return jsonify({"message": "Comentario agregado exitosamente.", "comment": inserted}), 201

    except ValidationError:
        raise
    except Exception:
        logger.exception("Error al agregar comentario")
        return internal_error("Error al agregar el comentario.")


@bp.route("/comments/<comment_id>", methods=["DELETE"])
@jwt_required()
@rate_limit(60)
def delete_comment(entity_type, entity_id, comment_id):
    try:
        validate_entity_type(entity_type)
        repos = _get_repos()

        current_user_email = get_jwt_identity()
        user = repos.users.find_by_email(current_user_email)
        if not user:
            return jsonify({"message": "Usuario no encontrado."}), 404

        user_id = str(user["_id"])

        entity_obj_id = _resolve_entity_read(entity_type, entity_id)
        if entity_obj_id is None:
            return jsonify({"message": "ID de entidad inválido."}), 400

        comment = repos.comments.find_for_entity(comment_id, entity_type, entity_obj_id)
        if not comment:
            return jsonify({"message": "Comentario no encontrado."}), 404
        if comment["user_id"] != user_id:
            return jsonify({"message": "No tienes permiso para eliminar este comentario."}), 403

        repos.comments.delete_by_id(comment_id)
        return jsonify({"message": "Comentario eliminado exitosamente."}), 200

    except ValidationError:
        raise
    except Exception:
        logger.exception("Error al eliminar comentario")
        return internal_error("Error al eliminar el comentario.")


@bp.route("/comments", methods=["GET"])
@jwt_required()
def get_comments(entity_type, entity_id):
    try:
        current_app.logger.info(f"Solicitud para obtener comentarios: entity_type={entity_type}, entity_id={entity_id}")
        validate_entity_type(entity_type)
        repos = _get_repos()

        entity_obj_id = _resolve_entity_read(entity_type, entity_id)
        if entity_obj_id is None:
            current_app.logger.warning(f"ID de entidad inválido: {entity_id}")
            return jsonify({"message": "ID de entidad inválido."}), 400

        page = get_int_arg("page", 1, minimum=1)
        limit = get_int_arg("limit", 10, minimum=1, maximum=100)
        skip = (page - 1) * limit

        comments_cursor = repos.comments.list_for_entity(entity_type, entity_obj_id, skip, limit)
        comments = []
        for comment in comments_cursor:
            try:
                comments.append(serialize_public_comment(comment, entity_type))
            except Exception:
                current_app.logger.error(f"Error al procesar el comentario {comment.get('_id')}")
                return internal_error("Error al procesar un comentario.")

        total_comments = repos.comments.count_for_entity(entity_type, entity_obj_id)
        total_pages = (total_comments + limit - 1) // limit

        current_app.logger.info(f"Comentarios obtenidos: {len(comments)} para la página {page}")
        return jsonify({
            "comments": comments,
            "pagination": {
                "total_comments": total_comments,
                "total_pages": total_pages,
                "current_page": page,
            },
        }), 200

    except ValidationError:
        raise
    except Exception:
        current_app.logger.error("Error al obtener los comentarios")
        return internal_error("Error al obtener los comentarios.")


@bp.route("/comments/<comment_id>/like", methods=["POST"])
@jwt_required()
@rate_limit(120)
def like_comment(entity_type, entity_id, comment_id):
    try:
        repos = _get_repos()
        current_user_email = get_jwt_identity()
        user = repos.users.find_by_email(current_user_email)
        if not user:
            return jsonify({"message": "Usuario no encontrado."}), 404
        user_id = str(user["_id"])

        validate_entity_type(entity_type)

        entity_obj_id = _resolve_entity_read(entity_type, entity_id)
        if entity_obj_id is None:
            return jsonify({"message": "ID de entidad inválido."}), 400

        comment = repos.comments.find_for_entity(comment_id, entity_type, entity_obj_id)
        if not comment:
            return jsonify({"message": "Comentario no encontrado."}), 404

        liked_by = comment.get("liked_by", [])
        disliked_by = comment.get("disliked_by", [])

        if user_id in liked_by:
            repos.comments.update_reaction(comment_id, {
                "$inc": {"likes": -1},
                "$pull": {"liked_by": user_id},
            })
            liked = False
        else:
            update_fields = {
                "$inc": {"likes": 1},
                "$addToSet": {"liked_by": user_id},
                "$pull": {"disliked_by": user_id},
            }
            if user_id in disliked_by:
                update_fields["$inc"]["dislikes"] = -1
            repos.comments.update_reaction(comment_id, update_fields)
            liked = True

        updated = serialize_public_comment(repos.comments.find_by_id(comment_id), entity_type)
        return jsonify({"message": "Like actualizado.", "comment": updated, "liked": liked}), 200

    except ValidationError:
        raise
    except Exception:
        logger.exception("Error en like_comment")
        return internal_error("Error al procesar el like.")


@bp.route("/comments/<comment_id>/dislike", methods=["POST"])
@jwt_required()
@rate_limit(120)
def dislike_comment(entity_type, entity_id, comment_id):
    try:
        repos = _get_repos()
        current_user_email = get_jwt_identity()
        user = repos.users.find_by_email(current_user_email)
        if not user:
            return jsonify({"message": "Usuario no encontrado."}), 404
        user_id = str(user["_id"])

        validate_entity_type(entity_type)

        entity_obj_id = _resolve_entity_read(entity_type, entity_id)
        if entity_obj_id is None:
            return jsonify({"message": "ID de entidad inválido."}), 400

        comment = repos.comments.find_for_entity(comment_id, entity_type, entity_obj_id)
        if not comment:
            return jsonify({"message": "Comentario no encontrado."}), 404

        liked_by = comment.get("liked_by", [])
        disliked_by = comment.get("disliked_by", [])

        if user_id in disliked_by:
            repos.comments.update_reaction(comment_id, {
                "$inc": {"dislikes": -1},
                "$pull": {"disliked_by": user_id},
            })
            disliked = False
        else:
            update_fields = {
                "$inc": {"dislikes": 1},
                "$addToSet": {"disliked_by": user_id},
                "$pull": {"liked_by": user_id},
            }
            if user_id in liked_by:
                update_fields["$inc"]["likes"] = -1
            repos.comments.update_reaction(comment_id, update_fields)
            disliked = True

        updated = serialize_public_comment(repos.comments.find_by_id(comment_id), entity_type)
        return jsonify({"message": "Dislike actualizado.", "comment": updated, "disliked": disliked}), 200

    except ValidationError:
        raise
    except Exception:
        logger.exception("Error en dislike_comment")
        return internal_error("Error al procesar el dislike.")
