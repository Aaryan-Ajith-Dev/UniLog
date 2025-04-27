from operations import set_row, get_row
from schema_utils import get_primary_keys
from db import get_connection
from datetime import datetime, timezone

def merge_log_operations(log_entries):
    for entry in log_entries:
        if entry.get("operation") != "SET":
            continue

        table_name = entry["table"]
        keys = entry.get("keys", {})
        item = entry.get("item", {})
        full_row = {**keys, **item}
        external_ts = datetime.fromtimestamp(entry["timestamp"], tz=timezone.utc)

        conn = get_connection()
        cur = conn.cursor()

        # Build WHERE clause using primary keys
        pks = get_primary_keys(table_name)
        where_clause = " AND ".join([f"{k} = %s" for k in keys.keys()])
        values = list(keys.values())

        # Check if row exists
        cur.execute(f"SELECT action_time FROM {table_name}_log WHERE action = 'SET' AND {where_clause} ORDER BY action_time DESC LIMIT 1", values)
        existing = cur.fetchone()

        update_needed = False
        if existing:
            existing_ts = existing[0]
            if external_ts > existing_ts:
                update_needed = True
        else:
            update_needed = True 

        if update_needed:
            set_row(table_name, full_row)

        cur.close()
        conn.close()
