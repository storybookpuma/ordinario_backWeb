create extension if not exists "pgcrypto";

create table if not exists app_users (
  id uuid primary key default gen_random_uuid(),
  username text not null unique,
  email text not null unique,
  password_hash text not null,
  profile_picture text not null default '/static/uploads/profile_pictures/default_picture.png',
  spotify_access_token text,
  spotify_refresh_token text,
  spotify_token_expires_at bigint,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists favorites (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references app_users(id) on delete cascade,
  entity_type text not null check (entity_type in ('song', 'album', 'artist')),
  entity_id text not null,
  name text,
  image text,
  created_at timestamptz not null default now(),
  unique (user_id, entity_type, entity_id)
);

create table if not exists comments (
  id uuid primary key default gen_random_uuid(),
  entity_type text not null check (entity_type in ('profile', 'song', 'album', 'artist')),
  entity_id text not null,
  user_id uuid not null references app_users(id) on delete cascade,
  comment_text text not null,
  created_at timestamptz not null default now()
);

create table if not exists comment_reactions (
  id uuid primary key default gen_random_uuid(),
  comment_id uuid not null references comments(id) on delete cascade,
  user_id uuid not null references app_users(id) on delete cascade,
  reaction text not null check (reaction in ('like', 'dislike')),
  created_at timestamptz not null default now(),
  unique (comment_id, user_id)
);

create table if not exists ratings (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references app_users(id) on delete cascade,
  entity_type text not null check (entity_type in ('song', 'album', 'artist')),
  entity_id text not null,
  rating int not null check (rating between 1 and 10),
  created_at timestamptz not null default now(),
  unique (user_id, entity_type, entity_id)
);

create table if not exists follows (
  follower_id uuid not null references app_users(id) on delete cascade,
  following_id uuid not null references app_users(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (follower_id, following_id),
  check (follower_id <> following_id)
);

create index if not exists idx_comments_entity on comments(entity_type, entity_id, created_at desc);
create index if not exists idx_comment_reactions_comment on comment_reactions(comment_id, reaction);
create index if not exists idx_favorites_user on favorites(user_id, entity_type);
create index if not exists idx_ratings_entity on ratings(entity_type, entity_id);
create index if not exists idx_follows_following on follows(following_id);
