from functools import wraps
from flask import jsonify, request, g
from marshmallow import ValidationError
import time

VALID_ENTITY_TYPES = {"profile", "song", "album", "artist"}
RATE_LIMIT_BUCKETS = {}
RATE_LIMIT_WINDOW_SECONDS = 60


def json_error(message, status_code=400, *, code=None):
    payload = {
        "message": message,
        "request_id": getattr(g, "request_id", None),
    }
    if code:
        payload["code"] = code
    return jsonify(payload), status_code


def internal_error(message="Error interno del servidor."):
    return json_error(message, 500, code="internal_error")


def get_json_body():
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else {}


def get_string_field(data, field, *, required=True, max_length=None):
    value = data.get(field, "")
    if not isinstance(value, str):
        raise ValidationError({field: ["Debe ser texto."]})

    value = value.strip()
    if required and not value:
        raise ValidationError({field: ["Este campo es obligatorio."]})
    if max_length and len(value) > max_length:
        raise ValidationError({field: [f"Debe tener máximo {max_length} caracteres."]})
    return value


def get_int_arg(name, default, *, minimum=0, maximum=None):
    raw_value = request.args.get(name, default)
    try:
        value = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValidationError({name: ["Debe ser un número entero."]}) from exc

    if value < minimum:
        raise ValidationError({name: [f"Debe ser mayor o igual a {minimum}."]})
    if maximum is not None and value > maximum:
        return maximum
    return value


def validate_entity_type(entity_type):
    if entity_type not in VALID_ENTITY_TYPES:
        raise ValidationError({"entity_type": [f"Debe ser uno de {sorted(VALID_ENTITY_TYPES)}."]})


def rate_limit(limit, window_seconds=RATE_LIMIT_WINDOW_SECONDS):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            identifier = request.headers.get("X-Forwarded-For", request.remote_addr or "unknown").split(",")[0].strip()
            key = (fn.__name__, identifier)
            now = time.time()
            bucket = [timestamp for timestamp in RATE_LIMIT_BUCKETS.get(key, []) if now - timestamp < window_seconds]

            if len(bucket) >= limit:
                RATE_LIMIT_BUCKETS[key] = bucket
                return json_error("Demasiadas solicitudes. Intenta de nuevo en unos segundos.", 429, code="rate_limited")

            bucket.append(now)
            RATE_LIMIT_BUCKETS[key] = bucket
            return fn(*args, **kwargs)
        return wrapper
    return decorator
