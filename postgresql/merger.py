from .operations import set_row
from .schema_utils import get_primary_keys
from .db import get_connection

def merge_log_operations(log_entries):
    # Step 1: Build a dictionary to hold the latest log for each unique keys
    latest_logs = {}

    for entry in log_entries:
        if entry.get("operation") != "SET":
            continue

        table_name = entry["table"]
        keys = entry.get("keys", {})
        item = entry.get("item", {})
        full_row = {**keys, **item}
        external_ts = int(entry["timestamp"])

        # Create a unique key for each primary keys set
        keys_tuple = tuple(sorted(keys.items()))

        # Store only if:
        # - it's the first time seeing this keys_tuple
        # - or it's a newer timestamp
        if keys_tuple not in latest_logs or external_ts > latest_logs[keys_tuple]["timestamp"]:
            latest_logs[keys_tuple] = {
                "table": table_name,
                "keys": keys,
                "row": full_row,
                "timestamp": external_ts
            }

    # Step 2: For each latest log, insert/update if necessary
    for info in latest_logs.values():
        table_name = info["table"]
        keys = info["keys"]
        full_row = info["row"]
        external_ts = info["timestamp"]

        conn = get_connection()
        cur = conn.cursor()

        # Build WHERE clause from primary keys
        pks = get_primary_keys(table_name)
        where_clause = " AND ".join([f"{k} = %s" for k in keys.keys()])
        values = list(keys.values())

        cur.execute(
            f"SELECT action_time FROM {table_name}_log WHERE action = 'SET' AND {where_clause} ORDER BY action_time DESC LIMIT 1",
            values
        )
        existing = cur.fetchone()

        update_needed = False
        if existing:
            existing_ts = existing[0]
            if external_ts > existing_ts:
                update_needed = True
        else:
            update_needed = True

        if update_needed:
            set_row(table_name, full_row, external_ts)

        cur.close()
        conn.close()
