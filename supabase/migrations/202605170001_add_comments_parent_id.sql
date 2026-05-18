alter table public.comments
add column if not exists parent_id uuid references public.comments(id) on delete cascade;

create index if not exists comments_parent_id_created_at_idx
on public.comments(parent_id, created_at);

create index if not exists comments_entity_top_level_idx
on public.comments(entity_type, entity_id, created_at desc)
where parent_id is null;
