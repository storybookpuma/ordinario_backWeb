create or replace function public.rating_summary(
  p_entity_type text,
  p_entity_id text
)
returns jsonb
language sql
stable
as $$
  with filtered as (
    select rating
    from public.ratings
    where entity_type = p_entity_type
      and entity_id = p_entity_id
  ),
  counts as (
    select rating, count(*)::int as count
    from filtered
    group by rating
  ),
  distribution as (
    select jsonb_object_agg(series.value::text, coalesce(counts.count, 0) order by series.value) as value
    from generate_series(1, 10) as series(value)
    left join counts on counts.rating = series.value
  )
  select jsonb_build_object(
    'averageRating', coalesce((select avg(rating)::float from filtered), 0),
    'ratingCount', coalesce((select count(*)::int from filtered), 0),
    'ratingDistribution', coalesce((select value from distribution), '{}'::jsonb)
  );
$$;
