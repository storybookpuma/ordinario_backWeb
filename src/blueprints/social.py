from flask import Blueprint, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from marshmallow import ValidationError
import logging

from ..utils.api import get_json_body, get_string_field, rate_limit, internal_error
from ..serializers import serialize_public_profile

logger = logging.getLogger(__name__)
bp = Blueprint("social", __name__)


def _get_repos():
    from flask import current_app
    return current_app.extensions["repositories"]


@bp.route("/follow_user", methods=["POST"])
@jwt_required()
@rate_limit(60)
def follow_user():
    try:
        repos = _get_repos()
        current_user_email = get_jwt_identity()
        current_user = repos.users.find_by_email(current_user_email)
        if not current_user:
            return jsonify({"message": "Usuario no encontrado"}), 404

        data = get_json_body()
        profile_id = get_string_field(data, "profile_id", max_length=120)

        target_user = repos.users.find_by_id(profile_id)
        if not target_user:
            return jsonify({"message": "Perfil no encontrado"}), 404

        current_user_id = current_user["_id"]
        repos.users.add_to_set_by_id(target_user["_id"], "followers", str(current_user_id))
        repos.users.add_to_set_by_id(current_user_id, "following", str(target_user["_id"]))

        return jsonify({"message": "Usuario seguido exitosamente"}), 200

    except ValidationError:
        raise
    except Exception:
        logger.exception("Error al seguir usuario")
        return internal_error("Error al seguir usuario.")


@bp.route("/unfollow_user", methods=["POST"])
@jwt_required()
@rate_limit(60)
def unfollow_user():
    try:
        repos = _get_repos()
        current_user_email = get_jwt_identity()
        current_user = repos.users.find_by_email(current_user_email)
        if not current_user:
            return jsonify({"message": "Usuario no encontrado"}), 404

        data = get_json_body()
        profile_id = get_string_field(data, "profile_id", max_length=120)

        target_user = repos.users.find_by_id(profile_id)
        if not target_user:
            return jsonify({"message": "Perfil no encontrado"}), 404

        current_user_id = current_user["_id"]
        repos.users.pull_by_id(target_user["_id"], "followers", str(current_user_id))
        repos.users.pull_by_id(current_user_id, "following", str(target_user["_id"]))

        return jsonify({"message": "Usuario dejado de seguir exitosamente"}), 200

    except ValidationError:
        raise
    except Exception:
        logger.exception("Error al dejar de seguir usuario")
        return internal_error("Error al dejar de seguir usuario.")


@bp.route("/get_following_details", methods=["POST"])
@jwt_required()
def get_following_details():
    repos = _get_repos()
    current_user_email = get_jwt_identity()
    current_user = repos.users.find_by_email(current_user_email)
    if not current_user:
        return jsonify({"message": "Usuario no encontrado"}), 404

    data = get_json_body()
    ids = data.get("ids", [])
    if not isinstance(ids, list):
        return jsonify({"message": "El campo 'ids' debe ser una lista."}), 400

    ids = [str(item) for item in ids[:100] if item]
    found_users = repos.users.find_many_by_ids(ids)
    users_list = [serialize_public_profile(u) for u in found_users]

    return jsonify({"users": users_list}), 200
