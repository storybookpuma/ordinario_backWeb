# Migration Scripts

## MongoDB to Supabase

1. Run `backend/supabase_schema.sql` in the Supabase SQL editor.
2. Configure environment variables:

```bash
export MONGO_URI="mongodb+srv://.../songbox"
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_SERVICE_ROLE_KEY="your-service-role-key"
```

3. Validate counts without writing:

```bash
python scripts/migrate_mongo_to_supabase.py --dry-run
```

4. Run the migration:

```bash
python scripts/migrate_mongo_to_supabase.py --output-id-map migration_id_map.json
```

The script writes MongoDB ObjectId to Supabase UUID mappings for users and comments.
