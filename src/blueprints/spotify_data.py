from collections import Counter, defaultdict
from datetime import datetime
from flask import Blueprint, jsonify, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity
import logging
import spotipy

from ..spotify_integration import get_valid_spotify_token
from ..utils.api import get_int_arg, internal_error, rate_limit

logger = logging.getLogger(__name__)
bp = Blueprint("spotify_data", __name__)


def _get_repos():
    return current_app.extensions["repositories"]


def _image_url(images):
    return (images or [{}])[0].get("url")


def _track_payload(track):
    album = track.get("album") or {}
    artists = track.get("artists") or []
    return {
        "spotifyId": track.get("id"),
        "name": track.get("name"),
        "image": _image_url(album.get("images")),
        "artist": ", ".join([artist.get("name", "") for artist in artists if artist.get("name")]),
        "album": album.get("name"),
        "spotifyUrl": (track.get("external_urls") or {}).get("spotify"),
        "popularity": track.get("popularity"),
        "durationMs": track.get("duration_ms"),
        "explicit": track.get("explicit"),
    }


def _album_payload(album):
    artists = album.get("artists") or []
    return {
        "spotifyId": album.get("id"),
        "name": album.get("name"),
        "image": _image_url(album.get("images")),
        "artist": ", ".join([artist.get("name", "") for artist in artists if artist.get("name")]),
        "spotifyUrl": (album.get("external_urls") or {}).get("spotify"),
        "releaseDate": album.get("release_date"),
        "albumType": album.get("album_type"),
        "totalTracks": album.get("total_tracks"),
    }


def _artist_payload(artist):
    return {
        "spotifyId": artist.get("id"),
        "name": artist.get("name"),
        "image": _image_url(artist.get("images")),
        "spotifyUrl": (artist.get("external_urls") or {}).get("spotify"),
        "genres": artist.get("genres") or [],
        "popularity": artist.get("popularity"),
        "followers": (artist.get("followers") or {}).get("total"),
    }


def _record_signal(repos, user_id, signal_type, entity_type, payload, strength, metadata=None, occurred_at=None):
    spotify_id = payload.get("spotifyId")
    if not spotify_id:
        return None
    try:
        repos.music_entities.upsert(
            entity_type,
            spotify_id,
            payload.get("name") or spotify_id,
            image=payload.get("image"),
            artist=payload.get("artist"),
            album=payload.get("album"),
            spotify_url=payload.get("spotifyUrl"),
            metadata=payload,
        )
    except Exception:
        logger.exception("Failed to upsert music entity")

    save_signal = getattr(repos.music_signals, "upsert", repos.music_signals.create)
    return save_signal(
        user_id,
        "spotify_api",
        signal_type,
        entity_type,
        entity_id=spotify_id,
        spotify_id=spotify_id,
        strength=strength,
        occurred_at=occurred_at,
        metadata={**payload, **(metadata or {})},
    )


@bp.route("/spotify/sync", methods=["POST"])
@jwt_required()
@rate_limit(10)
def spotify_sync():
    snapshot_id = None
    try:
        repos = _get_repos()
        user_email = get_jwt_identity()
        user = repos.users.find_by_email(user_email)
        if not user:
            return jsonify({"message": "Usuario no encontrado."}), 404

        access_token = get_valid_spotify_token(user_email, repos.users)
        if not access_token:
            return jsonify({"message": "Por favor, reconecta Spotify."}), 401

        sp = spotipy.Spotify(auth=access_token, requests_timeout=15)
        user_id = user["_id"]
        processed = defaultdict(int)
        snapshot = repos.spotify_sync_snapshots.create(user_id, "spotify_full_sync")
        snapshot_id = snapshot.get("id") or snapshot.get("_id")

        try:
            profile = sp.current_user()
            repos.spotify_profiles.upsert_for_user(user_id, profile)
            processed["profile"] += 1
        except Exception:
            logger.exception("Failed to sync Spotify profile")

        for time_range, strength in (("short_term", 0.82), ("medium_term", 0.72), ("long_term", 0.62)):
            try:
                for item in sp.current_user_top_tracks(limit=50, time_range=time_range).get("items", []):
                    _record_signal(repos, user_id, "top_track", "song", _track_payload(item), strength, {"timeRange": time_range})
                    processed[f"top_tracks_{time_range}"] += 1
            except Exception:
                logger.exception("Failed to sync top tracks %s", time_range)

            try:
                for item in sp.current_user_top_artists(limit=50, time_range=time_range).get("items", []):
                    _record_signal(repos, user_id, "top_artist", "artist", _artist_payload(item), strength, {"timeRange": time_range})
                    processed[f"top_artists_{time_range}"] += 1
            except Exception:
                logger.exception("Failed to sync top artists %s", time_range)

        try:
            for item in sp.current_user_recently_played(limit=50).get("items", []):
                track = item.get("track") or {}
                played_at = item.get("played_at")
                _record_signal(repos, user_id, "recent_play", "song", _track_payload(track), 0.25, {"playedAt": played_at, "context": item.get("context")}, played_at)
                processed["recently_played"] += 1
        except Exception:
            logger.exception("Failed to sync recently played")

        try:
            for item in sp.current_user_saved_tracks(limit=50).get("items", []):
                track = item.get("track") or {}
                _record_signal(repos, user_id, "saved_track", "song", _track_payload(track), 0.68, {"addedAt": item.get("added_at")}, item.get("added_at"))
                processed["saved_tracks"] += 1
        except Exception:
            logger.exception("Failed to sync saved tracks")

        try:
            for item in sp.current_user_saved_albums(limit=50).get("items", []):
                album = item.get("album") or {}
                _record_signal(repos, user_id, "saved_album", "album", _album_payload(album), 0.72, {"addedAt": item.get("added_at")}, item.get("added_at"))
                processed["saved_albums"] += 1
        except Exception:
            logger.exception("Failed to sync saved albums")

        try:
            followed = sp.current_user_followed_artists(limit=50).get("artists", {}).get("items", [])
            for item in followed:
                _record_signal(repos, user_id, "followed_artist", "artist", _artist_payload(item), 0.76)
                processed["followed_artists"] += 1
        except Exception:
            logger.exception("Failed to sync followed artists")

        repos.spotify_sync_snapshots.complete(snapshot_id, sum(processed.values()))
        return jsonify({"message": "Spotify sync completed.", "processed": dict(processed)}), 200

    except spotipy.exceptions.SpotifyException as error:
        logger.exception("Spotify sync failed")
        try:
            repos.spotify_sync_snapshots.fail(snapshot_id, error.msg or "Spotify sync failed.")
        except Exception:
            pass
        return jsonify({"message": error.msg or "Spotify sync failed."}), error.http_status or 500
    except Exception:
        logger.exception("Error during Spotify sync")
        try:
            repos.spotify_sync_snapshots.fail(snapshot_id, "Error al sincronizar Spotify.")
        except Exception:
            pass
        return internal_error("Error al sincronizar Spotify.")


@bp.route("/taste/wall", methods=["GET"])
@jwt_required()
@rate_limit(80)
def taste_wall():
    repos = _get_repos()
    user = repos.users.find_by_email(get_jwt_identity())
    if not user:
        return jsonify({"message": "Usuario no encontrado."}), 404

    limit = get_int_arg("limit", default=50, minimum=10, maximum=200)
    signals = repos.music_signals.recent_for_user(user["_id"], limit=limit)
    normalized = [_normalize_signal(signal) for signal in signals]

    type_counts = Counter(signal["entityType"] for signal in normalized if signal.get("entityType") in ("song", "album", "artist"))
    top_type = type_counts.most_common(1)[0][0] if type_counts else None
    current_era = {
        "song": "Track diary era",
        "album": "Album mode",
        "artist": "Artist deep-dive era",
    }.get(top_type, "Building a taste archive")

    seen = set()
    pinned = []
    for signal in sorted(normalized, key=lambda item: item.get("strength") or 0, reverse=True):
        key = f"{signal.get('entityType')}:{signal.get('entityId')}"
        if key in seen or signal.get("entityType") not in ("song", "album", "artist"):
            continue
        seen.add(key)
        pinned.append(_signal_to_item(signal))
        if len(pinned) == 4:
            break

    recent = []
    seen_recent = set()
    for signal in normalized:
        key = f"{signal.get('entityType')}:{signal.get('entityId')}"
        if key in seen_recent or signal.get("entityType") not in ("song", "album", "artist"):
            continue
        seen_recent.add(key)
        recent.append(_signal_to_item(signal))
        if len(recent) == 6:
            break

    return jsonify({
        "currentEra": current_era,
        "totalSignals": len(normalized),
        "dominantType": top_type.capitalize() + "s" if top_type else "None yet",
        "pinnedItems": pinned,
        "recentItems": recent,
        "sourceCounts": dict(Counter(signal.get("source") for signal in normalized if signal.get("source"))),
    }), 200


def _normalize_signal(signal):
    metadata = signal.get("metadata") or {}
    return {
        "source": signal.get("source"),
        "signalType": signal.get("signalType") or signal.get("signal_type"),
        "entityType": signal.get("entityType") or signal.get("entity_type"),
        "entityId": signal.get("entityId") or signal.get("entity_id"),
        "spotifyId": signal.get("spotifyId") or signal.get("spotify_id"),
        "strength": float(signal.get("strength") or 0),
        "occurredAt": str(signal.get("occurredAt") or signal.get("occurred_at") or ""),
        "metadata": metadata,
    }


def _signal_to_item(signal):
    metadata = signal.get("metadata") or {}
    return {
        "entityType": signal.get("entityType"),
        "entityId": signal.get("entityId") or signal.get("spotifyId"),
        "name": metadata.get("name") or signal.get("entityId"),
        "image": metadata.get("image"),
        "artist": metadata.get("artist"),
        "source": signal.get("source"),
        "signalType": signal.get("signalType"),
        "strength": signal.get("strength"),
    }
