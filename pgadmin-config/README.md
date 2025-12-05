# pgAdmin Server Configuration

This directory contains the pre-configured server connections for pgAdmin.

## Automatic Configuration

The `servers.json` file is automatically loaded by pgAdmin when the container is first started. The Docker Compose configuration mounts this file to `/pgadmin4/servers.json`, which is the default location pgAdmin checks for server definitions.

**Important Notes:**
- Servers are automatically added on first startup (when pgAdmin's configuration database is created)
- Password fields in `servers.json` are not imported by pgAdmin for security reasons
- On first connection to each server, you'll need to enter the password (`airflow`), which will then be saved

## Files

- `servers.json` - Server connection definitions (automatically loaded on first startup)
- `README.md` - This documentation file

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



