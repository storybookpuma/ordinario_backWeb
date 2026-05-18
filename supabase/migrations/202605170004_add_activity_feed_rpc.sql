create or replace function public.activity_feed(
  p_user_ids uuid[] default null,
  p_limit int default 20
)
returns table (
  type text,
  entity_type text,
  entity_id text,
  username text,
  user_photo text,
  text text,
  rating int,
  name text,
  image text,
  artist text,
  occurred_at timestamptz
)
language sql
stable
as $$
  select *
  from (
    select
      'comment'::text as type,
      c.entity_type,
      c.entity_id,
      coalesce(u.username, 'Usuario') as username,
      u.profile_picture as user_photo,
      c.comment_text as text,
      null::int as rating,
      c.name,
      c.image,
      c.artist,
      c.created_at as occurred_at
    from public.comments c
    left join public.app_users u on u.id = c.user_id
    where p_user_ids is null or c.user_id = any(p_user_ids)

    union all

    select
      'rating'::text as type,
      r.entity_type,
      r.entity_id,
      coalesce(u.username, 'Usuario') as username,
      u.profile_picture as user_photo,
      null::text as text,
      r.rating,
      r.name,
      r.image,
      r.artist,
      r.created_at as occurred_at
    from public.ratings r
    left join public.app_users u on u.id = r.user_id
    where p_user_ids is null or r.user_id = any(p_user_ids)

    union all

    select
      'favorite'::text as type,
      f.entity_type,
      f.entity_id,
      coalesce(u.username, 'Usuario') as username,
      u.profile_picture as user_photo,
      null::text as text,
      null::int as rating,
      f.name,
      f.image,
      f.artist,
      f.created_at as occurred_at
    from public.favorites f
    left join public.app_users u on u.id = f.user_id
    where p_user_ids is null or f.user_id = any(p_user_ids)
  ) activities
  order by occurred_at desc
  limit least(greatest(p_limit, 1), 50);
$$;
