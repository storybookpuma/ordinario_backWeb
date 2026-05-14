from .comments_repository import CommentsRepository
from .favorites_repository import FavoritesRepository
from .ratings_repository import RatingsRepository
from .supabase.comments_repository import SupabaseCommentsRepository
from .supabase.favorites_repository import SupabaseFavoritesRepository
from .supabase.ratings_repository import SupabaseRatingsRepository
from .supabase.users_repository import SupabaseUsersRepository
from .supabase_client import create_supabase_client
from .users_repository import UsersRepository


class Repositories:
    def __init__(self, users, comments, favorites, ratings):
        self.users = users
        self.comments = comments
        self.favorites = favorites
        self.ratings = ratings


def create_repositories(app, mongo):
    provider = app.config["DATABASE_PROVIDER"]

    if provider == "mongo":
        return Repositories(
            users=UsersRepository(mongo),
            comments=CommentsRepository(mongo),
            favorites=FavoritesRepository(mongo),
            ratings=RatingsRepository(mongo),
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
        )

    raise RuntimeError(f"Unsupported DATABASE_PROVIDER: {provider}")
