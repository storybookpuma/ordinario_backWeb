revoke execute on function public.rating_summary(text, text) from public;
revoke execute on function public.rating_summary(text, text) from anon;
revoke execute on function public.rating_summary(text, text) from authenticated;
grant execute on function public.rating_summary(text, text) to service_role;

revoke execute on function public.activity_feed(uuid[], int) from public;
revoke execute on function public.activity_feed(uuid[], int) from anon;
revoke execute on function public.activity_feed(uuid[], int) from authenticated;
grant execute on function public.activity_feed(uuid[], int) to service_role;
