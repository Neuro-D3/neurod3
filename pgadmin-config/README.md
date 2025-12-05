# pgAdmin Server Configuration

This directory contains the pre-configured server connections for pgAdmin.

## Files

- `servers.json` - Server connection definitions
- `init-pgadmin.sh` - Initialization script that copies the config on startup

## Troubleshooting

If servers don't appear in pgAdmin:

1. **Clear browser cache** - pgAdmin caches server lists
2. **Hard refresh** - Press Ctrl+Shift+R (or Cmd+Shift+R on Mac)
3. **Check logs** - Run `docker-compose logs pgadmin | grep -i "server\|config"`
4. **Manual verification** - Check if file exists:
   ```bash
   docker-compose exec pgadmin cat /var/lib/pgadmin/storage/admin@admin.com/servers.json
   ```
5. **Force reinstall** - Set `FORCE_OVERWRITE=true` in docker-compose.yml and restart

## Manual Server Addition

If automatic configuration doesn't work, you can manually add servers in pgAdmin:

1. Right-click "Servers" → "Register" → "Server"
2. Use these settings:
   - **Host**: `postgres` (not localhost!)
   - **Port**: `5432`
   - **Database**: `airflow` or `dag_data`
   - **Username**: `airflow`
   - **Password**: `airflow`



