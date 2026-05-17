import os


def _required_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _csv_env(name, default=""):
    raw_value = os.getenv(name, default)
    return [item.strip() for item in raw_value.split(",") if item.strip()]


class Config:
    APP_ENV = os.getenv("APP_ENV") or os.getenv("FLASK_ENV") or os.getenv("ENV", "development")
    IS_PRODUCTION = APP_ENV.lower() == "production"
    DATABASE_PROVIDER = os.getenv("DATABASE_PROVIDER", "mongo").lower()
    MONGO_URI = _required_env("MONGO_URI") if DATABASE_PROVIDER == "mongo" else os.getenv("MONGO_URI")
    JWT_SECRET_KEY = _required_env("JWT_SECRET_KEY")
    SECRET_KEY = _required_env("SECRET_KEY")
    MAX_CONTENT_LENGTH = int(os.getenv("MAX_CONTENT_LENGTH", 5 * 1024 * 1024))
    UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER", "static/uploads/profile_pictures")
    CORS_ORIGINS = _csv_env("CORS_ORIGINS")
    FRONTEND_DEEP_LINK = os.getenv("FRONTEND_DEEP_LINK", "frontsb://login")
    SPOTIFY_STATE_MAX_AGE_SECONDS = int(os.getenv("SPOTIFY_STATE_MAX_AGE_SECONDS", 600))
    SPOTIFY_EXCHANGE_CODE_MINUTES = int(os.getenv("SPOTIFY_EXCHANGE_CODE_MINUTES", os.getenv("SPOTIFY_CALLBACK_TOKEN_MINUTES", 10)))
    CREATE_DB_INDEXES = os.getenv("CREATE_DB_INDEXES", "true").lower() == "true"
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
