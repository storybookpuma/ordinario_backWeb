# spotify_integration.py

import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import time
import logging


logger = logging.getLogger(__name__)

def create_spotify_oauth(user_email):
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
    redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI")

    if not client_id or not client_secret or not redirect_uri:
        raise RuntimeError(
            "SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET and SPOTIFY_REDIRECT_URI are required."
        )

    return SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        scope="user-read-email user-top-read user-read-recently-played",
        cache_path=None
    )

def get_valid_spotify_token(user_email, users_repository):
    try:
        user = users_repository.find_by_email(user_email)
        if not user:
            logger.error("Usuario con email %s no encontrado en la base de datos.", user_email)
            return None

        token_info = {
            'access_token': user.get('spotify_access_token'),
            'refresh_token': user.get('spotify_refresh_token'),
            'expires_at': user.get('spotify_token_expires_at')
        }

        if not token_info['access_token']:
            logger.error("Token de acceso no encontrado para %s.", user_email)
            return None

        if token_info['expires_at'] - int(time.time()) < 60:
            logger.info("Intentando refrescar token de Spotify para %s.", user_email)
            sp_oauth = create_spotify_oauth(user_email)
            try:
                token_info = sp_oauth.refresh_access_token(token_info['refresh_token'])
                users_repository.update_by_email(
                    user_email,
                    {'$set': {
                        'spotify_access_token': token_info['access_token'],
                        'spotify_refresh_token': token_info.get('refresh_token', token_info['refresh_token']),
                        'spotify_token_expires_at': token_info['expires_at']
                    }}
                )
                logger.info("Token de Spotify actualizado para %s.", user_email)
            except Exception as e:
                logger.error("Error al refrescar el token de Spotify: %s", e)
                return None

        return token_info['access_token']

    except Exception as e:
        logger.error("Error en get_valid_spotify_token: %s", e)
        return None

def verify_entity_exists(entity_type, entity_id, sp):
    try:
        if entity_type == 'album':
            sp.album(entity_id)
        elif entity_type == 'artist':
            sp.artist(entity_id)
        elif entity_type == 'song':
            sp.track(entity_id)
        else:
            return False  # Tipo de entidad inválido
        return True  # Si no hay excepciones, la entidad existe
    except spotipy.exceptions.SpotifyException as e:
        logger.error("La entidad no existe en Spotify: %s", e)
        return False
