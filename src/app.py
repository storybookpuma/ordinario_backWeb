from flask import Flask, request, jsonify, redirect, g
from flask_pymongo import PyMongo
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timezone, timedelta
from flask_jwt_extended import JWTManager, create_access_token, get_jwt_identity, jwt_required
from marshmallow import Schema, fields, ValidationError
from dotenv import load_dotenv
from urllib.parse import urlencode
from collections import Counter
import uuid
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

load_dotenv()

from .spotify_integration import create_spotify_oauth, get_valid_spotify_token, verify_entity_exists
from .config import Config
from .repositories.factory import create_repositories
from .utils.api import json_error, internal_error, get_json_body, get_string_field, get_int_arg, validate_entity_type, rate_limit
from .utils.cache import TimedCache
from .serializers import serialize_current_user, serialize_public_profile, serialize_comment, serialize_public_comment
from werkzeug.utils import secure_filename
from flask_cors import CORS
import spotipy
import requests
import os
import logging

app = Flask(__name__)

# Configuración de Flask y MongoDB
app.config.from_object(Config)
app.secret_key = app.config["SECRET_KEY"]

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spotify_redirect_uri():
    proto = request.headers.get("X-Forwarded-Proto", request.scheme).split(",")[0].strip()
    host = request.headers.get("X-Forwarded-Host", request.host).split(",")[0].strip()
    return f"{proto}://{host}/callback"


def encode_spotify_state(email, return_url=None):
    if not email or len(email) > 120:
        raise ValueError("Invalid Spotify state email.")

    payload = {"email": email.lower()}
    if is_allowed_frontend_return_url(return_url):
        payload["return_url"] = return_url

    serializer = URLSafeTimedSerializer(app.config["SECRET_KEY"] or app.config["JWT_SECRET_KEY"])
    return serializer.dumps(payload, salt="spotify-oauth-state")


def decode_spotify_state(state):
    try:
        serializer = URLSafeTimedSerializer(app.config["SECRET_KEY"] or app.config["JWT_SECRET_KEY"])
        payload = serializer.loads(
            state,
            salt="spotify-oauth-state",
            max_age=app.config["SPOTIFY_STATE_MAX_AGE_SECONDS"],
        )
    except SignatureExpired as error:
        raise ValueError("Spotify state expired.") from error
    except (BadSignature, TypeError) as error:
        raise ValueError("Invalid Spotify state.") from error

    email = payload.get("email") if isinstance(payload, dict) else None
    return_url = payload.get("return_url") if isinstance(payload, dict) else None
    if not email or len(email) > 120:
        raise ValueError("Invalid Spotify state.")
    if return_url and not is_allowed_frontend_return_url(return_url):
        raise ValueError("Invalid Spotify return URL.")
    return email.lower(), return_url


def is_allowed_frontend_return_url(return_url):
    if not return_url:
        return False

    return return_url.startswith(("frontsb://", "exp://", "exps://"))


def build_frontend_redirect_url(return_url, params):
    base_url = return_url if is_allowed_frontend_return_url(return_url) else app.config["FRONTEND_DEEP_LINK"]
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{urlencode(params)}"


def create_session_token(email):
    return create_access_token(identity=email, expires_delta=timedelta(days=30))


def create_spotify_exchange_code(email):
    code = str(uuid.uuid4())
    app.extensions["spotify_auth_codes"].set(
        code,
        {"email": email},
        ttl=app.config["SPOTIFY_EXCHANGE_CODE_MINUTES"] * 60,
    )
    return code


def parse_timestamp(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def get_user_favorites(user):
    if not user:
        return []
    if app.config["DATABASE_PROVIDER"] == "mongo":
        return user.get("favorites", [])
    return favorites_repository.list_for_user(user)


def favorite_key(item):
    return f"{item.get('entityType')}:{item.get('entityId')}"


# Leer la clave de API desde las variables de entorno
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# Inicializar JWT y repositorios
mongo = PyMongo(app) if app.config["DATABASE_PROVIDER"] == "mongo" else None
repositories = create_repositories(app, mongo)
users_repository = repositories.users
comments_repository = repositories.comments
favorites_repository = repositories.favorites
ratings_repository = repositories.ratings

# Exponer repositorios y cache a blueprints vía extensions
app.extensions = getattr(app, "extensions", {})
app.extensions["repositories"] = repositories
app.extensions["cache"] = TimedCache(default_ttl=300)
app.extensions["spotify_auth_codes"] = TimedCache(default_ttl=600)

jwt = JWTManager(app)
if app.config["CORS_ORIGINS"]:
    CORS(app, origins=app.config["CORS_ORIGINS"])
elif not app.config["IS_PRODUCTION"]:
    CORS(app)
else:
    logger.warning("CORS_ORIGINS is unset in production; CORS headers are disabled.")


@app.before_request
def assign_request_id():
    g.request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())


@app.after_request
def add_request_id_header(response):
    response.headers["X-Request-ID"] = getattr(g, "request_id", "")
    return response


@app.errorhandler(ValidationError)
def handle_validation_error(e):
    return jsonify({"message": "Datos inválidos.", "errors": e.messages, "request_id": getattr(g, "request_id", None)}), 400


@app.errorhandler(404)
def handle_not_found(_error):
    return json_error("Ruta no encontrada.", 404, code="not_found")


@app.errorhandler(500)
def handle_unexpected_error(error):
    logger.exception("Unhandled server error", exc_info=error)
    return internal_error()

if app.config["CREATE_DB_INDEXES"]:
    ratings_repository.ensure_indexes()
    repositories.reviews.ensure_indexes()
    repositories.music_signals.ensure_indexes()


# Extensiones permitidas
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}

# Función para verificar si el archivo es válido
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def resolve_profile_entity_id(entity_id):
    return users_repository.get_profile_entity_id(entity_id)


# Registrar blueprints
from .blueprints import comments, favorites, ratings, reviews, social, spotify_data

app.register_blueprint(comments.bp)
app.register_blueprint(favorites.bp)
app.register_blueprint(ratings.bp)
app.register_blueprint(reviews.bp)
app.register_blueprint(social.bp)
app.register_blueprint(spotify_data.bp)


# Validación de datos de usuario con Marshmallow
class UserSchema(Schema):
    username = fields.Str(required=True)
    email = fields.Email(required=True)
    password = fields.Str(required=True)

user_schema = UserSchema()


@app.route('/healthz', methods=['GET'])
def healthz():
    return jsonify({"status": "ok", "request_id": getattr(g, "request_id", None)}), 200


@app.route('/readyz', methods=['GET'])
def readyz():
    try:
        if app.config["DATABASE_PROVIDER"] == "mongo" and mongo is not None:
            mongo.cx.admin.command('ping')
        return jsonify({
            "status": "ready",
            "database_provider": app.config["DATABASE_PROVIDER"],
            "request_id": getattr(g, "request_id", None),
        }), 200
    except Exception:
        logger.exception("Readiness check failed")
        return json_error("El servicio no está listo.", 503, code="not_ready")


# ------------------------------ Rutas de Usuario ----------------------------------

# Endpoint para registro
@app.route('/register', methods=['POST'])
@rate_limit(8)
def register():
    data = get_json_body()
    username = get_string_field(data, 'username', max_length=40)
    email = get_string_field(data, 'email', max_length=120).lower()
    password = get_string_field(data, 'password', max_length=128)
    user_schema.load({"username": username, "email": email, "password": password})

    # Verificar si el usuario ya existe
    existing_user = users_repository.find_by_email(email)
    if existing_user:
        return jsonify({'message': 'El usuario ya está registrado'}), 400

    # Crear el usuario
    hashed_password = generate_password_hash(password)
    user = {
        'username': username,
        'email': email,
        'password': hashed_password,
        'profile_picture': None,
        'favorites': []
    }
    created_user = users_repository.create(user)
    if getattr(created_user, 'inserted_id', None):
        user['_id'] = created_user.inserted_id

    # Crear el token JWT
    access_token = create_session_token(email)
    user_data = serialize_current_user(user)

    # Redirigir al flujo de autenticación de Spotify
    response = jsonify({'jwt': access_token, 'user': user_data})
    response.status_code = 201
    return response

# Endpoint para inicio de sesión
@app.route('/login', methods=['POST'])
@rate_limit(10)
def login():
    try:
        data = get_json_body()
        email = get_string_field(data, 'email', max_length=120).lower()
        password = get_string_field(data, 'password', max_length=128)

        # Validar si el usuario existe
        user = users_repository.find_by_email(email)
        if not user or not check_password_hash(user['password'], password):
            return jsonify({'message': 'Correo o contraseña incorrectos'}), 401

        # Crear un token JWT persistente para evitar pedir login en cada reinicio de app.
        jwt_token = create_session_token(email)

        # Devolver el token JWT y los datos del usuario
        user_data = serialize_current_user(user)
        return jsonify({
            "message": "Inicio de sesión exitoso",
            "jwt": jwt_token,
            "user": user_data
        }), 200

    except ValidationError as e:
        raise e
    except Exception as e:
        logger.exception("Error en login")
        return internal_error("Error al iniciar sesión.")

# Endpoint para iniciar la autenticación con Spotify
@app.route('/auth/spotify')
@rate_limit(20)
def auth_spotify():
    user_email = request.args.get('state')
    return_url = request.args.get('return_url')
    if not user_email:
        return jsonify({"error": "State parameter missing"}), 400
    try:
        sp_oauth = create_spotify_oauth(user_email, get_spotify_redirect_uri())
        auth_url = sp_oauth.get_authorize_url(state=encode_spotify_state(user_email, return_url))
        return redirect(auth_url)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 503

# Endpoint para manejar el callback de Spotify
@app.route('/callback')
def spotify_callback():
    code = request.args.get('code')
    state = request.args.get('state')

    if not code or not state:
        return jsonify({"error": "Faltan los parámetros 'code' o 'state'."}), 400

    try:
        user_email, return_url = decode_spotify_state(state)
    except ValueError as error:
        return jsonify({"error": str(error)}), 400

    try:
        # Obtener el token de acceso de Spotify
        sp_oauth = create_spotify_oauth(user_email, get_spotify_redirect_uri())
        token_info = sp_oauth.get_access_token(code, as_dict=True)

        # Actualizar los datos del usuario con los tokens de Spotify
        users_repository.update_by_email(
            user_email,
            {'$set': {
                'spotify_access_token': token_info['access_token'],
                'spotify_refresh_token': token_info.get('refresh_token'),
                'spotify_token_expires_at': token_info['expires_at']
            }}
        )

        exchange_code = create_spotify_exchange_code(user_email)

        # Redirigir al frontend utilizando un deep link
        redirect_url = build_frontend_redirect_url(return_url, {"spotify_code": exchange_code})
        return redirect(redirect_url)

    except Exception as e:
        return jsonify({"error": f"Error en el callback de Spotify: {str(e)}"}), 400


@app.route('/auth/spotify/exchange', methods=['POST'])
@rate_limit(20)
def exchange_spotify_auth_code():
    try:
        data = get_json_body()
        code = get_string_field(data, 'code', max_length=120)
        payload = app.extensions["spotify_auth_codes"].get(code)
        if not payload:
            return jsonify({"message": "Código de autenticación inválido o expirado."}), 400

        app.extensions["spotify_auth_codes"].delete(code)
        email = payload["email"]
        user = users_repository.find_by_email(email)
        if not user:
            return jsonify({"message": "Usuario no encontrado."}), 404

        token = create_session_token(email)
        return jsonify({"jwt": token, "user": serialize_current_user(user)}), 200
    except ValidationError:
        raise
    except Exception:
        logger.exception("Error exchanging Spotify auth code")
        return internal_error("Error al completar autenticación de Spotify.")


CONTENT_TYPE_MAP = {
    'png': 'image/png',
    'jpg': 'image/jpeg',
    'jpeg': 'image/jpeg',
    'gif': 'image/gif',
}

BUCKET_PROFILE_PICTURES = os.getenv("SUPABASE_BUCKET_PROFILE_PICTURES", "profile-pictures")


@app.route('/update_profile_picture', methods=['POST'])
@jwt_required()
@rate_limit(20)
def update_profile_picture():
    if 'profile_picture' not in request.files:
        return jsonify({'message': 'No se encontró el archivo en la solicitud'}), 400
    file = request.files['profile_picture']
    if file.filename == '':
        return jsonify({'message': 'No se seleccionó ningún archivo'}), 400
    if file and allowed_file(file.filename):
        extension = file.filename.rsplit('.', 1)[1].lower()
        user_email = get_jwt_identity()
        filename = f"user_{uuid.uuid4().hex}.{extension}"

        supabase = app.extensions.get("supabase")
        if not supabase:
            return internal_error("Storage no disponible.")

        try:
            file_bytes = file.read()
            supabase.upload_storage(
                BUCKET_PROFILE_PICTURES,
                filename,
                file_bytes,
                content_type=CONTENT_TYPE_MAP.get(extension, "application/octet-stream"),
            )
            public_url = supabase.public_storage_url(BUCKET_PROFILE_PICTURES, filename)
        except Exception:
            logger.exception("Error al subir imagen a Supabase Storage")
            return internal_error("Error al subir la imagen.")

        user = users_repository.find_by_email(user_email)
        if user:
            old_picture_url = user.get('profile_picture')
            if old_picture_url and BUCKET_PROFILE_PICTURES in old_picture_url:
                try:
                    # Extract path after bucket name from public URL
                    # URL format: .../storage/v1/object/public/{bucket}/{path}
                    old_path = old_picture_url.split(f"/public/{BUCKET_PROFILE_PICTURES}/", 1)[-1]
                    if old_path and old_path != old_picture_url:
                        supabase.delete_storage(BUCKET_PROFILE_PICTURES, old_path)
                except Exception:
                    logger.exception("Error al eliminar imagen anterior de Storage")

            users_repository.update_by_email(
                user_email,
                {'$set': {'profile_picture': public_url}}
            )
            return jsonify({'profile_picture': public_url}), 200
        else:
            return jsonify({'message': 'Usuario no encontrado'}), 404
    else:
        return jsonify({'message': 'Tipos de archivo permitidos: png, jpg, jpeg, gif'}), 400
    
@app.route('/update_username', methods=['POST'])
@jwt_required()
@rate_limit(20)
def update_username():
    data = get_json_body()
    new_username = get_string_field(data, 'username', max_length=40)
    # Verificar si el nombre de usuario ya existe
    existing_user = users_repository.find_by_username(new_username)
    if existing_user:
        return jsonify({'message': 'El nombre de usuario ya está en uso'}), 400
    user_email = get_jwt_identity()
    user = users_repository.find_by_email(user_email)
    if user:
        users_repository.update_by_email(
            user_email,
            {'$set': {'username': new_username}}
        )
        return jsonify({'username': new_username}), 200
    else:
        return jsonify({'message': 'Usuario no encontrado'}), 404


@app.route('/unlink_spotify', methods=['POST'])
@jwt_required()
@rate_limit(10)
def unlink_spotify():
    user_email = get_jwt_identity()
    user = users_repository.find_by_email(user_email)
    if not user:
        return jsonify({'message': 'Usuario no encontrado'}), 404

    users_repository.update_by_email(
        user_email,
        {'$set': {
            'spotify_access_token': None,
            'spotify_refresh_token': None,
            'spotify_token_expires_at': None,
        }}
    )
    updated_user = users_repository.find_by_email(user_email) or user
    return jsonify({'message': 'Spotify desvinculado.', 'user': serialize_current_user(updated_user)}), 200


# ------------------------------ Endpoints de SearchScreen ----------------------------------

@app.route('/me', methods=['GET'])
@jwt_required()
def get_current_user():
    try:
        current_user_email = get_jwt_identity()
        user = users_repository.find_by_email(current_user_email)
        if not user:
            return jsonify({"message": "Usuario no encontrado."}), 404

        user_data = serialize_current_user(user)

        return jsonify({"user": user_data}), 200

    except Exception as e:
        logger.exception("Error al obtener usuario actual")
        return internal_error("Error al obtener el usuario.")


@app.route('/search_song', methods=['GET'])
@jwt_required()
@rate_limit(120)
def search_song():
    query = request.args.get('q', '').strip()
    limit = get_int_arg('limit', 10, minimum=1, maximum=25)
    if not query:
        return jsonify({"message": "Se requiere un parámetro de búsqueda (q)"}), 400

    cache_key = f"search:song:{query}:{limit}"
    cached = app.extensions["cache"].get(cache_key)
    if cached is not None:
        return jsonify({"tracks": cached}), 200

    current_user = get_jwt_identity()
    access_token = get_valid_spotify_token(current_user, users_repository)
    if not access_token:
        return jsonify({"message": "Por favor, inicia sesión en Spotify para buscar canciones."}), 401

    try:
        sp = spotipy.Spotify(auth=access_token, requests_timeout=15)
        results = sp.search(q=query, type='track', limit=limit)
        tracks = []
        for item in results['tracks']['items']:
            track_info = {
                "id": item['id'],
                "name": item['name'],
                "artists": [artist['name'] for artist in item['artists']],
                "album": item['album']['name'],
                "url": item['external_urls']['spotify'],
                "preview_url": item.get('preview_url'),
                "cover_image": item['album']['images'][0]['url'] if item['album']['images'] else None
            }
            tracks.append(track_info)

        app.extensions["cache"].set(cache_key, tracks, ttl=300)
        return jsonify({"tracks": tracks}), 200

    except Exception as e:
        logger.exception("Error al buscar canción")
        return internal_error("Error al buscar la canción.")


@app.route('/search_album', methods=['GET'])
@jwt_required()
@rate_limit(120)
def search_album():
    try:
        query = request.args.get('q', '').strip()
        limit = get_int_arg('limit', 10, minimum=1, maximum=25)

        if not query:
            return jsonify({"message": "Se requiere un parámetro de búsqueda (q)"}), 400

        cache_key = f"search:album:{query}:{limit}"
        cached = app.extensions["cache"].get(cache_key)
        if cached is not None:
            return jsonify({"albums": cached}), 200

        current_user = get_jwt_identity()
        access_token = get_valid_spotify_token(current_user, users_repository)
        if not access_token:
            return jsonify({"message": "Por favor, inicia sesión en Spotify para buscar álbumes."}), 401

        sp = spotipy.Spotify(auth=access_token, requests_timeout=15)
        results = sp.search(q=query, type='album', limit=limit)
        albums = []
        for item in results['albums']['items']:
            album_info = {
                "id": item['id'],
                "name": item['name'],
                "artist": [artist['name'] for artist in item['artists']],
                "release_date": item['release_date'],
                "total_tracks": item['total_tracks'],
                "url": item['external_urls']['spotify'],
                "cover_image": item['images'][0]['url'] if item['images'] else None
            }
            albums.append(album_info)

        app.extensions["cache"].set(cache_key, albums, ttl=300)
        return jsonify({"albums": albums}), 200

    except ValidationError as e:
        raise e
    except Exception as e:
        app.logger.exception("Error al buscar álbum")
        return internal_error("Error al buscar el álbum.")

    
@app.route('/search_artist', methods=['GET'])
@jwt_required()
@rate_limit(120)
def search_artist():
    query = request.args.get('q', '').strip()
    limit = get_int_arg('limit', 10, minimum=1, maximum=25)
    if not query:
        return jsonify({"message": "Se requiere un parámetro de búsqueda (q)"}), 400

    cache_key = f"search:artist:{query}:{limit}"
    cached = app.extensions["cache"].get(cache_key)
    if cached is not None:
        return jsonify({"artists": cached}), 200

    current_user = get_jwt_identity()
    access_token = get_valid_spotify_token(current_user, users_repository)
    if not access_token:
        return jsonify({"message": "Por favor, inicia sesión en Spotify para buscar artistas."}), 401

    try:
        sp = spotipy.Spotify(auth=access_token, requests_timeout=15)
        results = sp.search(q=query, type='artist', limit=limit)
        artists = []
        for item in results['artists']['items']:
            artist_info = {
                "id": item['id'],
                "name": item['name'],
                "genres": item['genres'],
                "popularity": item['popularity'],
                "followers": item['followers']['total'],
                "url": item['external_urls']['spotify'],
                "image": item['images'][0]['url'] if item['images'] else None
            }
            artists.append(artist_info)

        app.extensions["cache"].set(cache_key, artists, ttl=300)
        return jsonify({"artists": artists}), 200

    except Exception as e:
        logger.exception("Error al buscar artista")
        return internal_error("Error al buscar el artista.")
    

@app.route('/search_playlist', methods=['GET'])
@jwt_required()
@rate_limit(120)
def search_playlist():
    query = request.args.get('q', '').strip()
    limit = get_int_arg('limit', 10, minimum=1, maximum=25)
    if not query:
        return jsonify({"message": "Se requiere un parámetro de búsqueda (q)"}), 400

    cache_key = f"search:playlist:{query}:{limit}"
    cached = app.extensions["cache"].get(cache_key)
    if cached is not None:
        return jsonify({"playlists": cached}), 200

    current_user = get_jwt_identity()
    access_token = get_valid_spotify_token(current_user, users_repository)
    if not access_token:
        return jsonify({"message": "Por favor, inicia sesión en Spotify para buscar playlists."}), 401

    try:
        sp = spotipy.Spotify(auth=access_token, requests_timeout=15)
        results = sp.search(q=query, type='playlist', limit=limit)
        playlists = []
        for item in results['playlists']['items']:
            playlist_info = {
                "id": item['id'],
                "name": item['name'],
                "owner": item['owner']['display_name'],
                "url": item['external_urls']['spotify'],
                "image": item['images'][0]['url'] if item['images'] else None,
                "description": item.get('description', ''),
            }
            playlists.append(playlist_info)

        app.extensions["cache"].set(cache_key, playlists, ttl=300)
        return jsonify({"playlists": playlists}), 200

    except Exception as e:
        logger.exception("Error al buscar playlist")
        return jsonify({"message": "Error al buscar las playlists."}), 500
    

@app.route('/search_profile', methods=['GET'])
@jwt_required()
@rate_limit(60)
def search_profile():
    query = request.args.get('q', '').strip()
    limit = get_int_arg('limit', 10, minimum=1, maximum=25)
    offset = get_int_arg('offset', 0, minimum=0)
    if not query:
        return jsonify({"message": "Se requiere un parámetro de búsqueda (q)"}), 400
    if len(query) > 80:
        return json_error("La búsqueda es demasiado larga.", 400, code="invalid_query")

    try:
        users = users_repository.search_profiles(query, limit, offset)
        profiles = [serialize_public_profile(user) for user in users]

        return jsonify({"profiles": profiles}), 200

    except Exception as e:
        logger.exception("Error al buscar perfiles")
        return internal_error("Error al buscar perfiles.")


# ------------------------------ Endpoints de HomeScreen ----------------------------------

@app.route('/top_albums_global', methods=['GET'])
@jwt_required()
@rate_limit(120)
def top_albums_global():
    limit = get_int_arg('limit', 20, minimum=1, maximum=50)
    offset = get_int_arg('offset', 0, minimum=0)

    cache_key = f"top:albums:{limit}:{offset}"
    cached = app.extensions["cache"].get(cache_key)
    if cached is not None:
        return jsonify({"albums": cached}), 200

    current_user = get_jwt_identity()
    access_token = get_valid_spotify_token(current_user, users_repository)
    if not access_token:
        logger.error("Token de acceso no disponible para /top_albums_global.")
        return jsonify({"message": "Por favor, inicia sesión en Spotify para ver los álbumes top."}), 401

    try:
        sp = spotipy.Spotify(auth=access_token, requests_timeout=15)
        new_releases = sp.new_releases(limit=limit, offset=offset, country='US')

        albums = []
        for album in new_releases['albums']['items']:
            album_info = {
                "id": album['id'],
                "name": album['name'],
                "artists": [artist['name'] for artist in album['artists']],
                "url": album['external_urls']['spotify'],
                "cover_image": album['images'][0]['url'] if album['images'] else None,
                "type": album['album_type']
            }
            albums.append(album_info)

        app.extensions["cache"].set(cache_key, albums, ttl=600)
        return jsonify({"albums": albums}), 200

    except spotipy.exceptions.SpotifyException as e:
        logger.error("SpotifyException en /top_albums_global: %s", e)
        return jsonify({"message": f"Error con la API de Spotify: {e.msg}"}), e.http_status
    except Exception as e:
        logger.error("Error general en /top_albums_global: %s", e)
        return internal_error("Error al obtener los álbumes top.")


@app.route('/top_artists_global', methods=['GET'])
@jwt_required()
@rate_limit(120)
def top_artists_global():
    limit = get_int_arg('limit', 20, minimum=1, maximum=50)
    offset = get_int_arg('offset', 0, minimum=0)

    cache_key = f"top:artists:{limit}:{offset}"
    cached = app.extensions["cache"].get(cache_key)
    if cached is not None:
        return jsonify({"artists": cached}), 200

    current_user = get_jwt_identity()
    access_token = get_valid_spotify_token(current_user, users_repository)
    if not access_token:
        logger.error("Token de acceso no disponible para /top_artists_global.")
        return jsonify({"message": "Por favor, inicia sesión en Spotify para ver los artistas top."}), 401

    try:
        sp = spotipy.Spotify(auth=access_token, requests_timeout=30)
        new_releases = sp.new_releases(limit=50, country='US')

        artists_dict = {}
        for album in new_releases['albums']['items']:
            for artist in album['artists']:
                artist_id = artist['id']
                if artist_id not in artists_dict:
                    artists_dict[artist_id] = {
                        "id": artist_id,
                        "name": artist['name'],
                        "image": None,
                        "url": None,
                        "popularity": 0
                    }

        artist_ids = list(artists_dict.keys())
        for i in range(0, len(artist_ids), 50):
            batch_ids = artist_ids[i:i + 50]
            try:
                artists_info = sp.artists(batch_ids)['artists']
            except requests.exceptions.ReadTimeout:
                logger.error("Timeout al obtener detalles de artistas.")
                return jsonify({"message": "La solicitud a Spotify ha tardado demasiado. Por favor, intenta nuevamente más tarde."}), 504
            except spotipy.exceptions.SpotifyException as e:
                logger.error("SpotifyException en detalles de artistas: %s", e)
                return jsonify({"message": f"Error con la API de Spotify: {e.msg}"}), e.http_status

            for artist_info in artists_info:
                if artist_info:
                    artists_dict[artist_info['id']]['image'] = artist_info['images'][0]['url'] if artist_info['images'] else None
                    artists_dict[artist_info['id']]['url'] = artist_info['external_urls']['spotify']
                    artists_dict[artist_info['id']]['popularity'] = artist_info.get('popularity', 0)

        artists_list = list(artists_dict.values())
        artists_sorted = sorted(artists_list, key=lambda x: x['popularity'], reverse=True)
        paginated_artists = artists_sorted[offset:offset + limit]

        response_artists = []
        for artist in paginated_artists:
            artist_info = {
                "id": artist['id'],
                "name": artist['name'],
                "image": artist['image'],
                "url": artist['url']
            }
            response_artists.append(artist_info)

        app.extensions["cache"].set(cache_key, response_artists, ttl=600)
        return jsonify({"artists": response_artists}), 200

    except requests.exceptions.ReadTimeout:
        logger.error("Timeout al obtener nuevos lanzamientos de Spotify.")
        return jsonify({"message": "La solicitud a Spotify ha tardado demasiado. Por favor, intenta nuevamente más tarde."}), 504
    except spotipy.exceptions.SpotifyException as e:
        logger.error("SpotifyException en /top_artists_global: %s", e)
        return jsonify({"message": f"Error con la API de Spotify: {e.msg}"}), e.http_status
    except Exception as e:
        logger.error("Error general en /top_artists_global: %s", e)
        return internal_error("Error al obtener los artistas top.")


@app.route('/videos', methods=['GET'])
@rate_limit(60)
def get_videos():
    cache_key = "videos:music"
    cached = app.extensions["cache"].get(cache_key)
    if cached is not None:
        return jsonify({"videos": cached}), 200

    youtube_url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet",
        "q": "music",
        "type": "video",
        "videoCategoryId": "10",
        "regionCode": "US",
        "maxResults": 10,
        "key": YOUTUBE_API_KEY
    }

    try:
        response = requests.get(youtube_url, params=params, timeout=15)
        response.raise_for_status()

        videos = response.json().get("items", [])
        formatted_videos = [
            {
                "title": video['snippet']['title'],
                "channel": video['snippet']['channelTitle'],
                "thumbnail": video['snippet']['thumbnails']['high']['url'],
                "videoId": video['id']['videoId'],
                "url": f"https://www.youtube.com/watch?v={video['id']['videoId']}"
            }
            for video in videos
        ]
        app.extensions["cache"].set(cache_key, formatted_videos, ttl=600)
        return jsonify({"videos": formatted_videos}), 200

    except requests.exceptions.HTTPError as errh:
        logger.error("HTTP Error al obtener videos: %s", errh)
        logger.error("YouTube response content: %s", response.text)
        return jsonify({"error": "Error al obtener los videos", "details": response.text}), response.status_code
    except requests.exceptions.RequestException as err:
        logger.error("Error al obtener videos: %s", err)
        return jsonify({"error": "Error al obtener los videos"}), 500


@app.route('/song_details', methods=['GET'])
@jwt_required()
@rate_limit(120)
def song_details():
    song_id = request.args.get('song_id')
    if not song_id:
        return jsonify({"message": "Se requiere el ID de la canción."}), 400

    cache_key = f"details:song:{song_id}"
    cached = app.extensions["cache"].get(cache_key)
    if cached is not None:
        return jsonify({'song': cached}), 200

    current_user_email = get_jwt_identity()
    access_token = get_valid_spotify_token(current_user_email, users_repository)
    if not access_token:
        return jsonify({"message": "Por favor, inicia sesión en Spotify."}), 401

    try:
        sp = spotipy.Spotify(auth=access_token, requests_timeout=15)
        track = sp.track(song_id)

        song_info = {
            "id": track['id'],
            "name": track['name'],
            "artists": [artist['name'] for artist in track['artists']],
            "artist_ids": [artist['id'] for artist in track['artists']],
            "album": track['album']['name'],
            "album_id": track['album']['id'],
            "cover_image": track['album']['images'][0]['url'] if track['album']['images'] else None,
            "duration_ms": track['duration_ms'],
            "popularity": track['popularity'],
            "preview_url": track['preview_url'],
            "url": track['external_urls']['spotify'],
            "release_date": track['album']['release_date'],
        }

        # Obtener detalles adicionales de los artistas
        artist_ids = song_info['artist_ids']
        artists = sp.artists(artist_ids)['artists']
        genres = []
        followers = 0
        for artist in artists:
            genres.extend(artist['genres'])
            followers += artist['followers']['total']
        song_info['genres'] = genres
        song_info['followers'] = followers

        rating_summary = ratings_repository.summarize_entity('song', song_id)
        song_info['averageRating'] = rating_summary['averageRating']
        song_info['ratingCount'] = rating_summary['ratingCount']
        song_info['ratingDistribution'] = rating_summary['ratingDistribution']

        app.extensions["cache"].set(cache_key, song_info, ttl=600)
        return jsonify({'song': song_info}), 200

    except spotipy.exceptions.SpotifyException as e:
        return jsonify({"message": f"Error con la API de Spotify: {e.msg}"}), e.http_status
    except Exception as e:
        logger.exception("Error al obtener detalles de canción")
        return internal_error("Error al obtener los detalles de la canción.")


@app.route('/album_details', methods=['GET'])
@jwt_required()
@rate_limit(120)
def album_details():
    album_id = request.args.get('album_id')
    if not album_id:
        return jsonify({"message": "Se requiere el ID del álbum."}), 400

    cache_key = f"details:album:{album_id}"
    cached = app.extensions["cache"].get(cache_key)
    if cached is not None:
        return jsonify({'album': cached}), 200

    current_user_email = get_jwt_identity()
    access_token = get_valid_spotify_token(current_user_email, users_repository)
    if not access_token:
        return jsonify({"message": "Por favor, inicia sesión en Spotify."}), 401

    try:
        sp = spotipy.Spotify(auth=access_token, requests_timeout=15)
        album = sp.album(album_id)

        album_info = {
            "id": album['id'],
            "name": album['name'],
            "artists": [artist['name'] for artist in album['artists']],
            "artist_ids": [artist['id'] for artist in album['artists']],
            "cover_image": album['images'][0]['url'] if album['images'] else None,
            "release_date": album['release_date'],
            "total_tracks": album['total_tracks'],
            "url": album['external_urls']['spotify'],
            "tracks": []
        }

        for track in album['tracks']['items']:
            track_info = {
                "id": track['id'],
                "name": track['name'],
                "duration_ms": track['duration_ms'],
                "preview_url": track['preview_url'],
                "url": track['external_urls']['spotify'],
                "track_number": track['track_number'],
                "artists": [artist['name'] for artist in track['artists']],
                "artist_ids": [artist['id'] for artist in track['artists']],
            }
            album_info['tracks'].append(track_info)

        rating_summary = ratings_repository.summarize_entity('album', album_id)
        album_info['averageRating'] = rating_summary['averageRating']
        album_info['ratingCount'] = rating_summary['ratingCount']
        album_info['ratingDistribution'] = rating_summary['ratingDistribution']

        app.extensions["cache"].set(cache_key, album_info, ttl=600)
        return jsonify({'album': album_info}), 200

    except spotipy.exceptions.SpotifyException as e:
        return jsonify({"message": f"Error con la API de Spotify: {e.msg}"}), e.http_status
    except Exception as e:
        logger.exception("Error al obtener detalles de álbum")
        return internal_error("Error al obtener los detalles del álbum.")


@app.route('/artist_details', methods=['GET'])
@jwt_required()
@rate_limit(120)
def artist_details():
    artist_id = request.args.get('artist_id')
    if not artist_id:
        return jsonify({"message": "Se requiere el ID del artista."}), 400

    cache_key = f"details:artist:{artist_id}"
    cached = app.extensions["cache"].get(cache_key)
    if cached is not None:
        return jsonify({'artist': cached['artist'], 'albums': cached['albums']}), 200

    current_user_email = get_jwt_identity()
    access_token = get_valid_spotify_token(current_user_email, users_repository)
    if not access_token:
        return jsonify({"message": "Por favor, inicia sesión en Spotify."}), 401

    try:
        sp = spotipy.Spotify(auth=access_token, requests_timeout=15)
        artist = sp.artist(artist_id)
        albums = sp.artist_albums(artist_id, album_type='album')['items']

        artist_info = {
            'id': artist['id'],
            'name': artist['name'],
            'image': artist['images'][0]['url'] if artist['images'] else None,
            'genres': artist['genres'],
            'popularity': artist['popularity'],
            'followers': artist['followers']['total'],
        }

        albums_info = []
        for album in albums:
            albums_info.append({
                'id': album['id'],
                'title': album['name'],
                'image': album['images'][0]['url'] if album['images'] else None,
                'release_date': album['release_date'],
            })

        rating_summary = ratings_repository.summarize_entity('artist', artist_id)
        artist_info['averageRating'] = rating_summary['averageRating']
        artist_info['ratingCount'] = rating_summary['ratingCount']
        artist_info['ratingDistribution'] = rating_summary['ratingDistribution']

        app.extensions["cache"].set(cache_key, {'artist': artist_info, 'albums': albums_info}, ttl=600)
        return jsonify({'artist': artist_info, 'albums': albums_info}), 200

    except spotipy.exceptions.SpotifyException as e:
        return jsonify({"message": f"Error con la API de Spotify: {e.msg}"}), e.http_status
    except Exception as e:
        logger.exception("Error al obtener detalles de artista")
        return internal_error("Error al obtener los detalles del artista.")
    

@app.route('/profile_details', methods=['GET'])
@jwt_required()
def get_profile_details():
    profile_id = request.args.get('profile_id')
    if not profile_id:
        return jsonify({"message": "Se requiere profile_id"}), 400

    try:
        user = users_repository.find_by_id(profile_id)
        if not user:
            return jsonify({"message": "Perfil no encontrado"}), 404

        profile_data = serialize_public_profile(user, include_favorites=True)
        profile_data["favorites"] = get_user_favorites(user)
        profile_data["comments_enabled"] = True
        return jsonify(profile_data), 200
    except Exception as e:
        logger.exception("Error al cargar perfil")
        return internal_error("Error al cargar el perfil.")


@app.route('/public_profile/<username>', methods=['GET'])
@rate_limit(60)
def public_profile(username):
    username = (username or '').strip()
    if not username:
        return jsonify({"message": "Se requiere username"}), 400

    try:
        user = users_repository.find_by_username(username)
        if not user:
            return jsonify({"message": "Perfil no encontrado"}), 404

        favorites = get_user_favorites(user)
        profile_data = serialize_public_profile(user)
        profile_data["favorites"] = favorites[:24]
        profile_data["counts"] = {
            "favorites": len(favorites),
            "followers": len(user.get("followers", []) or []),
            "following": len(user.get("following", []) or []),
        }
        return jsonify(profile_data), 200
    except Exception as e:
        logger.exception("Error al cargar perfil público")
        return internal_error("Error al cargar el perfil público.")


@app.route('/profile_compatibility', methods=['GET'])
@jwt_required()
@rate_limit(60)
def profile_compatibility():
    profile_id = request.args.get('profile_id')
    if not profile_id:
        return jsonify({"message": "Se requiere profile_id"}), 400

    try:
        current_user = users_repository.find_by_email(get_jwt_identity())
        target_user = users_repository.find_by_id(profile_id)
        if not current_user or not target_user:
            return jsonify({"message": "Perfil no encontrado"}), 404

        current_favorites = get_user_favorites(current_user)
        target_favorites = get_user_favorites(target_user)

        current_by_key = {favorite_key(item): item for item in current_favorites if item.get("entityId")}
        target_by_key = {favorite_key(item): item for item in target_favorites if item.get("entityId")}
        shared_keys = set(current_by_key.keys()) & set(target_by_key.keys())
        union_count = len(set(current_by_key.keys()) | set(target_by_key.keys()))

        current_artists = Counter()
        target_artists = Counter()
        for bucket, favorites in ((current_artists, current_favorites), (target_artists, target_favorites)):
            for item in favorites:
                if item.get("entityType") == "artist" and item.get("name"):
                    bucket[item["name"]] += 2
                if item.get("artist"):
                    for artist in [part.strip() for part in item["artist"].split(",") if part.strip()]:
                        bucket[artist] += 1

        shared_artist_names = set(current_artists.keys()) & set(target_artists.keys())
        shared_artist_weight = sum(min(current_artists[name], target_artists[name]) for name in shared_artist_names)
        artist_pool = sum((current_artists | target_artists).values()) or 1
        item_score = (len(shared_keys) / union_count) if union_count else 0
        artist_score = shared_artist_weight / artist_pool
        # Ratings similarity
        current_ratings = ratings_repository.collection.find({"userId": str(current_user["_id"])}) if app.config["DATABASE_PROVIDER"] == "mongo" else repositories.ratings.client.select("ratings", user_id=str(current_user["_id"]), limit=1000)
        target_ratings = ratings_repository.collection.find({"userId": str(target_user["_id"])}) if app.config["DATABASE_PROVIDER"] == "mongo" else repositories.ratings.client.select("ratings", user_id=str(target_user["_id"]), limit=1000)

        def normalize_rating(row):
            return {
                "entityType": row.get("entity_type") or row.get("entityType"),
                "entityId": row.get("entity_id") or row.get("entityId"),
                "rating": float(row.get("rating") or 0),
                "name": row.get("name"),
                "image": row.get("image"),
                "artist": row.get("artist"),
            }

        current_ratings_norm = [normalize_rating(r) for r in current_ratings]
        target_ratings_norm = [normalize_rating(r) for r in target_ratings]

        current_rating_by_key = {f"{r['entityType']}:{r['entityId']}": r for r in current_ratings_norm if r.get("entityId")}
        target_rating_by_key = {f"{r['entityType']}:{r['entityId']}": r for r in target_ratings_norm if r.get("entityId")}
        shared_rating_keys = set(current_rating_by_key.keys()) & set(target_rating_by_key.keys())

        rating_similarity = 0
        closest_ratings = []
        if shared_rating_keys:
            diffs = []
            for key in shared_rating_keys:
                cr = current_rating_by_key[key]
                tr = target_rating_by_key[key]
                diff = abs(cr["rating"] - tr["rating"])
                diffs.append(diff)
                closest_ratings.append({
                    "entityType": cr["entityType"],
                    "entityId": cr["entityId"],
                    "name": cr.get("name") or tr.get("name"),
                    "image": cr.get("image") or tr.get("image"),
                    "artist": cr.get("artist") or tr.get("artist"),
                    "yourRating": cr["rating"],
                    "theirRating": tr["rating"],
                    "diff": diff,
                })
            avg_diff = sum(diffs) / len(diffs)
            # max possible diff is 9 (1 vs 10), normalize to 0-1 where 0 diff = 1.0
            rating_similarity = max(0, 1 - (avg_diff / 9))
            closest_ratings = sorted(closest_ratings, key=lambda x: x["diff"])[:5]

        # Weighted score: 55% favorites, 25% artists, 20% ratings
        score = round((item_score * 0.55 + artist_score * 0.25 + rating_similarity * 0.20) * 100)
        if str(current_user["_id"]) == str(target_user["_id"]):
            score = 100

        def get_taste_label(s):
            if s >= 80:
                return "Soulmate"
            if s >= 60:
                return "Strong match"
            if s >= 40:
                return "Some overlap"
            if s >= 20:
                return "Different lanes"
            return "Polar opposites"

        shared_items = [target_by_key[key] for key in list(shared_keys)[:8]]
        top_shared_artists = sorted(
            [{"name": name, "count": min(current_artists[name], target_artists[name])} for name in shared_artist_names],
            key=lambda item: (-item["count"], item["name"]),
        )[:5]

        return jsonify({
            "score": score,
            "sharedCount": len(shared_keys),
            "totalCompared": union_count,
            "sharedItems": shared_items,
            "topSharedArtists": top_shared_artists,
            "tasteLabel": get_taste_label(score),
            "sharedRatingsCount": len(shared_rating_keys),
            "closestRatings": closest_ratings[:5],
        }), 200
    except Exception as e:
        logger.exception("Error al calcular compatibilidad")
        return internal_error("Error al calcular compatibilidad musical.")




@app.route('/album_tracks', methods=['GET'])
@jwt_required()
@rate_limit(120)
def get_album_tracks():
    album_id = request.args.get('album_id')
    if not album_id:
        return jsonify({"message": "Se requiere el ID del álbum."}), 400

    cache_key = f"tracks:album:{album_id}"
    cached = app.extensions["cache"].get(cache_key)
    if cached is not None:
        return jsonify({"tracks": cached}), 200

    current_user = get_jwt_identity()
    access_token = get_valid_spotify_token(current_user, users_repository)
    if not access_token:
        return jsonify({"message": "Por favor, inicia sesión en Spotify para ver las canciones del álbum."}), 401

    try:
        sp = spotipy.Spotify(auth=access_token, requests_timeout=15)
        album_tracks_results = sp.album_tracks(album_id)

        tracks = []
        for item in album_tracks_results['items']:
            track_info = {
                "name": item['name'],
                "track_number": item['track_number'],
                "duration_ms": item['duration_ms'],
                "preview_url": item.get('preview_url'),
                "url": item['external_urls']['spotify']
            }
            tracks.append(track_info)

        app.extensions["cache"].set(cache_key, tracks, ttl=600)
        return jsonify({"tracks": tracks}), 200

    except Exception as e:
        logger.exception("Error al obtener canciones del álbum")
        return internal_error("Error al obtener las canciones del álbum.")


# Comments, favorites, ratings, and social routes have been moved to blueprints.



@app.route('/recently_listened', methods=['GET'])
@jwt_required()
@rate_limit(60)
def recently_listened():
    user_email = get_jwt_identity()
    cache_key = f"recently_listened:{user_email}"
    cached = app.extensions["cache"].get(cache_key)
    if cached is not None:
        return jsonify({'songs': cached}), 200

    access_token = get_valid_spotify_token(user_email, users_repository)
    if not access_token:
        return jsonify({
            'is_playing': False,
            'item': None,
            'requires_reconnect': True,
            'message': 'Por favor, vuelve a conectar Spotify.',
        }), 200

    try:
        sp = spotipy.Spotify(auth=access_token, requests_timeout=15)
        recently_played = sp.current_user_recently_played(limit=10)
        songs = []
        for item in recently_played['items']:
            track = item['track']
            songs.append({
                'id': track['id'],
                'name': track['name'],
                'artist': ', '.join([artist['name'] for artist in track['artists']]),
                'album': track['album']['name'],
                'cover_image': track['album']['images'][0]['url'] if track['album']['images'] else None,
                'url': track['external_urls']['spotify'],
            })
        app.extensions["cache"].set(cache_key, songs, ttl=300)
        return jsonify({'songs': songs}), 200
    except Exception as e:
        logger.exception("Error al obtener recently listened")
        return jsonify({'error': 'Error al obtener canciones reproducidas recientemente.'}), 500


@app.route('/spotify/currently_playing', methods=['GET'])
@jwt_required()
@rate_limit(60)
def spotify_currently_playing():
    user_email = get_jwt_identity()
    access_token = get_valid_spotify_token(user_email, users_repository)
    if not access_token:
        return jsonify({'error': 'Por favor, inicia sesión en Spotify.'}), 401

    try:
        sp = spotipy.Spotify(auth=access_token, requests_timeout=15)
        current = sp.current_user_playing_track()
        if not current or not current.get('item'):
            return jsonify({'is_playing': False, 'item': None, 'requires_reconnect': False}), 200

        track = current['item']
        if track.get('type') != 'track' or not track.get('album'):
            return jsonify({
                'is_playing': current.get('is_playing', False),
                'item': None,
                'requires_reconnect': False,
                'message': 'El contenido actual no es una canción.',
            }), 200

        item = {
            'id': track['id'],
            'name': track['name'],
            'artists': [artist['name'] for artist in track['artists']],
            'album': track['album']['name'],
            'cover_image': track['album']['images'][0]['url'] if track['album']['images'] else None,
            'url': track['external_urls']['spotify'],
        }
        return jsonify({'is_playing': current.get('is_playing', False), 'item': item, 'requires_reconnect': False}), 200
    except spotipy.exceptions.SpotifyException as e:
        if e.http_status in (401, 403):
            return jsonify({
                'is_playing': False,
                'item': None,
                'requires_reconnect': True,
                'message': 'Vuelve a conectar Spotify para ver lo que escuchas.',
            }), 200
        logger.exception("Error de Spotify al obtener currently playing")
        return jsonify({'is_playing': False, 'item': None, 'requires_reconnect': False, 'message': 'Spotify no respondió.'}), 200
    except Exception as e:
        logger.exception("Error al obtener currently playing")
        return jsonify({'is_playing': False, 'item': None, 'requires_reconnect': False, 'message': 'Error al obtener la canción actual.'}), 200


@app.route('/charts/top_rated', methods=['GET'])
@jwt_required()
@rate_limit(60)
def top_rated_charts():
    entity_type = request.args.get('entityType', '').strip()
    limit = get_int_arg('limit', 20, minimum=1, maximum=50)

    if entity_type not in ('song', 'album', 'artist'):
        return json_error("Tipo de entidad inválido.", 400, code="invalid_entity_type")

    cache_key = f"charts:top_rated:{entity_type}:{limit}"
    cached = app.extensions["cache"].get(cache_key)
    if cached is not None:
        return jsonify({"entityType": entity_type, "items": cached}), 200

    try:
        top_items = ratings_repository.top_rated(entity_type, limit)
        entity_ids = [item["_id"] for item in top_items]

        # Enriquecer con metadata de Spotify si hay token disponible
        metadata = {}
        current_user = get_jwt_identity()
        access_token = get_valid_spotify_token(current_user, users_repository)
        if access_token and entity_ids:
            try:
                sp = spotipy.Spotify(auth=access_token, requests_timeout=15)
                if entity_type == 'song':
                    tracks = sp.tracks(entity_ids).get('tracks', [])
                    for track in tracks:
                        if track:
                            metadata[track['id']] = {
                                'name': track['name'],
                                'image': track['album']['images'][0]['url'] if track['album']['images'] else None,
                                'artist': ', '.join(a['name'] for a in track['artists']),
                            }
                elif entity_type == 'album':
                    albums = sp.albums(entity_ids).get('albums', [])
                    for album in albums:
                        if album:
                            metadata[album['id']] = {
                                'name': album['name'],
                                'image': album['images'][0]['url'] if album['images'] else None,
                                'artist': ', '.join(a['name'] for a in album['artists']),
                            }
                elif entity_type == 'artist':
                    artists = sp.artists(entity_ids).get('artists', [])
                    for artist in artists:
                        if artist:
                            metadata[artist['id']] = {
                                'name': artist['name'],
                                'image': artist['images'][0]['url'] if artist['images'] else None,
                            }
            except Exception:
                logger.exception("Error al enriquecer charts con Spotify")

        results = []
        for item in top_items:
            meta = metadata.get(item["_id"], {})
            results.append({
                "entityId": item["_id"],
                "averageRating": round(item["averageRating"], 2) if item["averageRating"] else 0,
                "ratingCount": item["ratingCount"],
                "name": meta.get("name") or item.get("name") or item["_id"],
                "image": meta.get("image") or item.get("image"),
                "artist": meta.get("artist") or item.get("artist"),
            })

        app.extensions["cache"].set(cache_key, results, ttl=600)
        return jsonify({"entityType": entity_type, "items": results}), 200

    except Exception as e:
        logger.exception("Error al obtener charts")
        return internal_error("Error al obtener los charts.")


@app.route('/wrapped/monthly', methods=['GET'])
@jwt_required()
@rate_limit(30)
def monthly_wrapped():
    user_email = get_jwt_identity()
    current_user = users_repository.find_by_email(user_email)
    if not current_user:
        return jsonify({"message": "Usuario no encontrado."}), 404

    month_param = request.args.get("month")
    now = datetime.now(timezone.utc)
    try:
        if month_param:
            target_date = datetime.strptime(month_param, "%Y-%m")
            target_year = target_date.year
            target_month = target_date.month
        else:
            target_year = now.year
            target_month = now.month
    except ValueError:
        return json_error("Formato de mes inválido. Usa YYYY-MM.", 400, code="invalid_month")

    user_id = str(current_user["_id"])
    cache_key = f"wrapped:monthly:{user_id}:{target_year:04d}-{target_month:02d}"
    cached = app.extensions["cache"].get(cache_key)
    if cached is not None:
        return jsonify(cached), 200

    def is_target_month(value):
        ts = parse_timestamp(value)
        if not ts:
            return False
        return ts.year == target_year and ts.month == target_month

    def normalize_rating(row):
        return {
            "entityType": row.get("entity_type") or row.get("entityType"),
            "entityId": row.get("entity_id") or row.get("entityId"),
            "rating": row.get("rating") or 0,
            "name": row.get("name"),
            "image": row.get("image"),
            "artist": row.get("artist"),
            "timestamp": row.get("created_at") or row.get("timestamp"),
        }

    def normalize_favorite(row):
        return {
            "entityType": row.get("entity_type") or row.get("entityType"),
            "entityId": row.get("entity_id") or row.get("entityId"),
            "name": row.get("name"),
            "image": row.get("image"),
            "artist": row.get("artist"),
            "timestamp": row.get("created_at") or row.get("timestamp"),
        }

    try:
        if app.config["DATABASE_PROVIDER"] == "mongo":
            rating_rows = list(ratings_repository.collection.find({"userId": user_id}).sort("timestamp", -1).limit(1000))
            favorite_rows = favorites_repository.list_for_user(current_user)
        else:
            rating_rows = repositories.ratings.client.select(
                "ratings",
                user_id=user_id,
                limit=1000,
                order="created_at.desc",
            )
            favorite_rows = repositories.favorites.client.select(
                "favorites",
                user_id=user_id,
                limit=1000,
                order="created_at.desc",
            )

        monthly_ratings = [normalize_rating(row) for row in rating_rows if is_target_month(row.get("created_at") or row.get("timestamp"))]
        all_favorites = [normalize_favorite(row) for row in favorite_rows]
        monthly_favorites = [fav for fav in all_favorites if is_target_month(fav.get("timestamp"))]

        rating_values = [float(item["rating"] or 0) for item in monthly_ratings]
        type_counts = Counter(item["entityType"] for item in monthly_ratings if item.get("entityType"))
        favorite_type_counts = Counter(item["entityType"] for item in all_favorites if item.get("entityType"))

        artist_counter = Counter()
        for item in monthly_ratings + all_favorites:
            if item.get("entityType") == "artist" and item.get("name"):
                artist_counter[item["name"]] += 1
            if item.get("artist"):
                for artist in [part.strip() for part in item["artist"].split(",") if part.strip()]:
                    artist_counter[artist] += 1

        top_rated = sorted(
            monthly_ratings,
            key=lambda item: (item.get("rating") or 0, str(item.get("timestamp") or "")),
            reverse=True,
        )[:5]

        # Compare with previous month
        prev_date = datetime(target_year, target_month, 1) - timedelta(days=1)
        prev_year = prev_date.year
        prev_month = prev_date.month

        def is_prev_month(value):
            ts = parse_timestamp(value)
            if not ts:
                return False
            return ts.year == prev_year and ts.month == prev_month

        prev_monthly_ratings = [normalize_rating(row) for row in rating_rows if is_prev_month(row.get("created_at") or row.get("timestamp"))]
        prev_monthly_favorites = [fav for fav in all_favorites if is_prev_month(fav.get("timestamp"))]
        prev_rating_values = [float(item["rating"] or 0) for item in prev_monthly_ratings]
        prev_type_counts = Counter(item["entityType"] for item in prev_monthly_ratings if item.get("entityType"))

        def delta(current, previous):
            return current - previous

        comparison = {
            "previousMonth": f"{prev_year:04d}-{prev_month:02d}",
            "hasPrevious": len(prev_monthly_ratings) > 0 or len(prev_monthly_favorites) > 0,
            "ratingsDelta": delta(len(monthly_ratings), len(prev_monthly_ratings)),
            "averageRatingDelta": round(
                (sum(rating_values) / len(rating_values) if rating_values else 0)
                - (sum(prev_rating_values) / len(prev_rating_values) if prev_rating_values else 0),
                2,
            ),
            "newFavoritesDelta": delta(len(monthly_favorites), len(prev_monthly_favorites)),
            "dominantTypeChanged": (
                type_counts.most_common(1)[0][0] if type_counts else None
            ) != (
                prev_type_counts.most_common(1)[0][0] if prev_type_counts else None
            ),
        }

        wrapped = {
            "month": f"{target_year:04d}-{target_month:02d}",
            "summary": {
                "ratingsCount": len(monthly_ratings),
                "averageRating": round(sum(rating_values) / len(rating_values), 2) if rating_values else 0,
                "favoritesCount": len(all_favorites),
                "newFavoritesCount": len(monthly_favorites),
                "topEntityType": type_counts.most_common(1)[0][0] if type_counts else None,
            },
            "comparison": comparison,
            "ratingsByType": dict(type_counts),
            "favoritesByType": dict(favorite_type_counts),
            "topArtists": [{"name": name, "count": count} for name, count in artist_counter.most_common(5)],
            "topRated": top_rated,
            "recentFavorites": monthly_favorites[:5],
        }

        app.extensions["cache"].set(cache_key, wrapped, ttl=300)
        return jsonify(wrapped), 200
    except Exception as e:
        logger.exception("Error al obtener monthly wrapped")
        return internal_error("Error al obtener tu resumen mensual.")


@app.route('/badges', methods=['GET'])
@jwt_required()
@rate_limit(30)
def user_badges():
    user_email = get_jwt_identity()
    current_user = users_repository.find_by_email(user_email)
    if not current_user:
        return jsonify({"message": "Usuario no encontrado."}), 404

    user_id = str(current_user["_id"])
    cache_key = f"badges:{user_id}"
    cached = app.extensions["cache"].get(cache_key)
    if cached is not None:
        return jsonify({"badges": cached}), 200

    try:
        if app.config["DATABASE_PROVIDER"] == "mongo":
            rating_rows = list(ratings_repository.collection.find({"userId": user_id}))
            favorite_rows = favorites_repository.list_for_user(current_user)
        else:
            rating_rows = repositories.ratings.client.select("ratings", user_id=user_id, limit=1000)
            favorite_rows = repositories.favorites.client.select("favorites", user_id=user_id, limit=1000)

        def normalize_rating(row):
            return {
                "rating": float(row.get("rating") or 0),
                "entityType": row.get("entity_type") or row.get("entityType"),
                "artist": row.get("artist"),
                "timestamp": parse_timestamp(row.get("created_at") or row.get("timestamp")),
            }

        ratings = [normalize_rating(r) for r in rating_rows]
        favorites = favorite_rows
        following = current_user.get("following", []) or []

        album_ratings = [r for r in ratings if r.get("entityType") == "album"]
        song_ratings = [r for r in ratings if r.get("entityType") == "song"]
        rating_values = [r["rating"] for r in ratings if r["rating"]]
        avg_rating = sum(rating_values) / len(rating_values) if rating_values else 0

        unique_artists = set()
        for r in ratings:
            if r.get("artist"):
                for a in [part.strip() for part in r["artist"].split(",") if part.strip()]:
                    unique_artists.add(a)

        night_ratings = [r for r in ratings if r.get("timestamp") and 0 <= r["timestamp"].hour < 5]

        months_with_activity = set()
        for r in ratings:
            if r.get("timestamp"):
                months_with_activity.add((r["timestamp"].year, r["timestamp"].month))

        user_created = parse_timestamp(current_user.get("created_at"))
        account_age_days = (datetime.now(timezone.utc) - user_created).days if user_created else 0

        badges = []

        def add_badge(bid, name, description, icon, rarity, condition):
            if condition:
                badges.append({"id": bid, "name": name, "description": description, "icon": icon, "unlocked": True, "rarity": rarity})

        add_badge("album_hunter", "Album Hunter", "Rated 10+ albums", "headphones", "common", len(album_ratings) >= 10)
        add_badge("song_critic", "Song Critic", "Rated 20+ songs", "music", "common", len(song_ratings) >= 20)
        add_badge("explorer", "Explorer", "Discovered 20+ unique artists", "globe", "common", len(unique_artists) >= 20)
        add_badge("collector", "Collector", "Saved 20+ favorites", "heart", "common", len(favorites) >= 20)
        add_badge("night_owl", "Night Owl", "Rated music after midnight", "moon-o", "uncommon", len(night_ratings) >= 5)
        add_badge("harsh_critic", "Harsh Critic", "Average rating below 4.0", "frown-o", "rare", avg_rating > 0 and avg_rating < 4.0 and len(rating_values) >= 10)
        add_badge("generous_rater", "Generous Rater", "Average rating above 8.5", "smile-o", "rare", avg_rating > 8.5 and len(rating_values) >= 10)
        add_badge("social_butterfly", "Social Butterfly", "Following 10+ users", "users", "common", len(following) >= 10)
        add_badge("consistent", "Consistent", "Active in 3+ different months", "calendar", "uncommon", len(months_with_activity) >= 3)
        add_badge("veteran", "Veteran", "6+ months on SongBox", "star", "epic", account_age_days >= 180)

        app.extensions["cache"].set(cache_key, badges, ttl=300)
        return jsonify({"badges": badges}), 200
    except Exception as e:
        logger.exception("Error al obtener badges")
        return internal_error("Error al obtener tus badges.")


@app.route('/activity', methods=['GET'])
@jwt_required()
@rate_limit(60)
def activity_feed():
    limit = get_int_arg('limit', 20, minimum=1, maximum=50)
    scope = request.args.get('scope', 'global').strip()

    user_email = get_jwt_identity()
    current_user = users_repository.find_by_email(user_email)
    if not current_user:
        return jsonify({"message": "Usuario no encontrado."}), 404

    current_user_id = str(current_user["_id"])
    is_personalized = scope == 'personalized'

    if is_personalized:
        following_ids = current_user.get('following', [])
        relevant_ids = list(set(following_ids + [current_user_id]))
        if not relevant_ids:
            return jsonify({"activities": []}), 200
        cache_key = f"activity:personalized:{current_user_id}:{limit}"
    else:
        relevant_ids = None
        cache_key = f"activity:global:{limit}"

    cached = app.extensions["cache"].get(cache_key)
    if cached is not None:
        return jsonify({"activities": cached}), 200

    try:
        activities = []

        if app.config["DATABASE_PROVIDER"] == "mongo":
            if is_personalized:
                comment_filter = {'user_id': {'$in': relevant_ids}}
                rating_filter = {'userId': {'$in': relevant_ids}}
            else:
                comment_filter = {}
                rating_filter = {}

            recent_comments = comments_repository.collection.find(comment_filter).sort('timestamp', -1).limit(limit)
            for c in recent_comments:
                activities.append({
                    "type": "comment",
                    "entityType": c.get("entity_type"),
                    "entityId": str(c.get("entity_id")),
                    "username": c.get("username"),
                    "userPhoto": c.get("user_photo"),
                    "text": c.get("comment_text"),
                    "name": c.get("name"),
                    "image": c.get("image"),
                    "artist": c.get("artist"),
                    "timestamp": c.get("timestamp").isoformat() if c.get("timestamp") else None,
                })

            recent_ratings = ratings_repository.collection.find(rating_filter).sort('timestamp', -1).limit(limit)
            for r in recent_ratings:
                user = users_repository.find_by_id(r.get("userId"))
                activities.append({
                    "type": "rating",
                    "entityType": r.get("entityType"),
                    "entityId": r.get("entityId"),
                    "username": user.get("username") if user else "Usuario",
                    "userPhoto": user.get("profile_picture") if user else None,
                    "rating": r.get("rating"),
                    "name": r.get("name"),
                    "image": r.get("image"),
                    "artist": r.get("artist"),
                    "timestamp": r.get("timestamp").isoformat() if r.get("timestamp") else None,
                })
        else:
            # Supabase path: fetch recent items from each table
            comment_kwargs = {"limit": limit, "order": "created_at.desc"}
            rating_kwargs = {"limit": limit, "order": "created_at.desc"}
            favorite_kwargs = {"limit": limit, "order": "created_at.desc"}

            if is_personalized:
                ids_csv = ",".join(relevant_ids)
                comment_kwargs["user_id_in"] = ids_csv
                rating_kwargs["user_id_in"] = ids_csv
                favorite_kwargs["user_id_in"] = ids_csv

            comment_rows = repositories.comments.client.select("comments", **comment_kwargs)
            for row in comment_rows:
                user = users_repository.find_by_id(row.get("user_id"))
                activities.append({
                    "type": "comment",
                    "entityType": row.get("entity_type"),
                    "entityId": row.get("entity_id"),
                    "username": user.get("username") if user else "Usuario",
                    "userPhoto": user.get("profile_picture") if user else None,
                    "text": row.get("comment_text"),
                    "name": row.get("name"),
                    "image": row.get("image"),
                    "artist": row.get("artist"),
                    "timestamp": row.get("created_at"),
                })

            rating_rows = repositories.ratings.client.select("ratings", **rating_kwargs)
            for row in rating_rows:
                user = users_repository.find_by_id(row.get("user_id"))
                activities.append({
                    "type": "rating",
                    "entityType": row.get("entity_type"),
                    "entityId": row.get("entity_id"),
                    "username": user.get("username") if user else "Usuario",
                    "userPhoto": user.get("profile_picture") if user else None,
                    "rating": row.get("rating"),
                    "name": row.get("name"),
                    "image": row.get("image"),
                    "artist": row.get("artist"),
                    "timestamp": row.get("created_at"),
                })

            favorite_rows = repositories.favorites.client.select("favorites", **favorite_kwargs)
            for row in favorite_rows:
                user = users_repository.find_by_id(row.get("user_id"))
                activities.append({
                    "type": "favorite",
                    "entityType": row.get("entity_type"),
                    "entityId": row.get("entity_id"),
                    "username": user.get("username") if user else "Usuario",
                    "userPhoto": user.get("profile_picture") if user else None,
                    "name": row.get("name"),
                    "image": row.get("image"),
                    "artist": row.get("artist"),
                    "timestamp": row.get("created_at"),
                })

        activities.sort(key=lambda x: x.get("timestamp") or "", reverse=True)
        activities = activities[:limit]

        app.extensions["cache"].set(cache_key, activities, ttl=300)
        return jsonify({"activities": activities}), 200

    except Exception as e:
        logger.exception("Error al obtener activity feed")
        return internal_error("Error al obtener el feed de actividad.")


# Ratings and social routes have been moved to blueprints.



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
