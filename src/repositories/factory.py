from .comments_repository import CommentsRepository
from .favorites_repository import FavoritesRepository
from .music_signals_repository import MusicSignalsRepository
from .music_entities_repository import MusicEntitiesRepository
from .ratings_repository import RatingsRepository
from .reviews_repository import ReviewsRepository
from .spotify_profiles_repository import SpotifyProfilesRepository
from .spotify_sync_snapshots_repository import SpotifySyncSnapshotsRepository
from .supabase.comments_repository import SupabaseCommentsRepository
from .supabase.favorites_repository import SupabaseFavoritesRepository
from .supabase.music_signals_repository import SupabaseMusicSignalsRepository
from .supabase.music_entities_repository import SupabaseMusicEntitiesRepository
from .supabase.ratings_repository import SupabaseRatingsRepository
from .supabase.reviews_repository import SupabaseReviewsRepository
from .supabase.spotify_profiles_repository import SupabaseSpotifyProfilesRepository
from .supabase.spotify_sync_snapshots_repository import SupabaseSpotifySyncSnapshotsRepository
from .supabase.users_repository import SupabaseUsersRepository
from .supabase_client import create_supabase_client
from .users_repository import UsersRepository


class Repositories:
    def __init__(self, users, comments, favorites, ratings, reviews, music_signals, spotify_profiles, music_entities, spotify_sync_snapshots):
        self.users = users
        self.comments = comments
        self.favorites = favorites
        self.ratings = ratings
        self.reviews = reviews
        self.music_signals = music_signals
        self.spotify_profiles = spotify_profiles
        self.music_entities = music_entities
        self.spotify_sync_snapshots = spotify_sync_snapshots


def create_repositories(app, mongo):
    provider = app.config["DATABASE_PROVIDER"]

    if provider == "mongo":
        return Repositories(
            users=UsersRepository(mongo),
            comments=CommentsRepository(mongo),
            favorites=FavoritesRepository(mongo),
            ratings=RatingsRepository(mongo),
            reviews=ReviewsRepository(mongo),
            music_signals=MusicSignalsRepository(mongo),
            spotify_profiles=SpotifyProfilesRepository(mongo),
            music_entities=MusicEntitiesRepository(mongo),
            spotify_sync_snapshots=SpotifySyncSnapshotsRepository(mongo),
        )

    if provider == "supabase":
        client = create_supabase_client(app)
        app.extensions = getattr(app, "extensions", {})
        app.extensions["supabase"] = client
        return Repositories(
            users=SupabaseUsersRepository(client),
            comments=SupabaseCommentsRepository(client),
            favorites=SupabaseFavoritesRepository(client),
            ratings=SupabaseRatingsRepository(client),
            reviews=SupabaseReviewsRepository(client),
            music_signals=SupabaseMusicSignalsRepository(client),
            spotify_profiles=SupabaseSpotifyProfilesRepository(client),
            music_entities=SupabaseMusicEntitiesRepository(client),
            spotify_sync_snapshots=SupabaseSpotifySyncSnapshotsRepository(client),
        )

    raise RuntimeError(f"Unsupported DATABASE_PROVIDER: {provider}")
