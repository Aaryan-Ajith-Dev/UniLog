from .db import get_connection
from .schema_utils import get_primary_keys
from .log_table_manager import create_log_table


def set_row(table_name, row_dict, action_time):
    cols = list(row_dict.keys())
    values = [row_dict[col] for col in cols]
    pks = get_primary_keys(table_name)

    conn = get_connection()
    cur = conn.cursor()

    # Build WHERE clause for primary keys to check existing data
    where_clause = " AND ".join([f"{k} = %s" for k in pks])
    pk_values = [row_dict[k] for k in pks]

    # Check existing latest action_time from log
    cur.execute(f"""
    SELECT action_time FROM {table_name}_log 
    WHERE {where_clause} AND action = 'SET'
    ORDER BY action_time DESC LIMIT 1
    """, pk_values)
    existing_action_time = cur.fetchone()

    if existing_action_time:
        latest_action_time = existing_action_time[0]
        if action_time <= latest_action_time:
            print(f"Skipping outdated SET operation for {row_dict} with action_time {action_time}")
            cur.close()
            conn.close()
            return

    # --- Fetch existing row from table to fill missing non-PK fields ---
    cur.execute(f"SELECT * FROM {table_name} WHERE {where_clause}", pk_values)
    existing_row = cur.fetchone()
    colnames = [desc[0] for desc in cur.description]

    complete_row = {}
    if existing_row:
        db_row = dict(zip(colnames, existing_row))
        # Fill complete row: prefer new data from row_dict, else from database
        for col in colnames:
            if col in row_dict:
                complete_row[col] = row_dict[col]
            else:
                complete_row[col] = db_row[col]
    else:
        # No existing row, use only provided data
        complete_row = row_dict

    # Proceed with UPSERT
    all_cols = list(complete_row.keys())
    all_values = [complete_row[col] for col in all_cols]
    update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in all_cols])

    insert_sql = f"""
    INSERT INTO {table_name} ({','.join(all_cols)})
    VALUES ({','.join(['%s'] * len(all_values))})
    ON CONFLICT ({','.join(pks)}) DO UPDATE SET
    {update_set};
    """
    cur.execute(insert_sql, all_values)

    # Log full row (not just partial update!)
    log_sql = f"""
    INSERT INTO {table_name}_log ({','.join(all_cols)}, action, action_time)
    VALUES ({','.join(['%s'] * len(all_values))}, %s, %s)
    """
    cur.execute(log_sql, all_values + ['SET', action_time])

    conn.commit()
    cur.close()
    conn.close()

def get_row(table_name, filters, action_time):
    """
    Perform the GET operation on the specified table and log it.
    The action_time is passed as a Unix timestamp integer.
    """
    create_log_table(table_name)
    where_clause = " AND ".join([f"{col} = %s" for col in filters])
    values = list(filters.values())

    conn = get_connection()
    cur = conn.cursor()
    
    # Check the most recent action_time for the GET operation
    cur.execute(f"""
    SELECT action_time FROM {table_name}_log 
    WHERE {where_clause} AND action = 'GET'
    ORDER BY action_time DESC LIMIT 1
    """, values)
    
    # Get the latest action_time
    existing_action_time = cur.fetchone()

    # If the action_time is outdated, skip logging the GET operation
    if existing_action_time:
        latest_action_time = existing_action_time[0]
        if action_time <= latest_action_time:
            print(f"Skipping outdated GET operation for {filters}")
            cur.close()
            conn.close()
            return 

    # Execute the GET query
    cur.execute(f"SELECT * FROM {table_name} WHERE {where_clause}", values)
    result = cur.fetchall()

    # Log the GET operation
    filter_cols = list(filters.keys())
    log_sql = f"""
    INSERT INTO {table_name}_log ({','.join(filter_cols)}, action, action_time)
    VALUES ({','.join(['%s'] * len(values))}, %s, %s)
    """
    cur.execute(log_sql, values + ['GET', action_time])

    conn.commit()
    cur.close()
    conn.close()
    return result
