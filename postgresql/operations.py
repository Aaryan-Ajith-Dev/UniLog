from .db import get_connection
from .schema_utils import get_primary_keys
from .log_table_manager import create_log_table


def set_row(table_name, row_dict, action_time):
    # Extract columns and values from the row_dict
    cols = list(row_dict.keys())
    values = [row_dict[col] for col in cols]
    pks = get_primary_keys(table_name)

    # Build WHERE clause for primary keys to check if record exists
    where_clause = " AND ".join([f"{k} = %s" for k in row_dict.keys()])
    values_for_check = list(row_dict.values())

    conn = get_connection()
    cur = conn.cursor()

    # Check the most recent action_time in the log for this record
    cur.execute(f"""
    SELECT action_time FROM {table_name}_log 
    WHERE {where_clause} AND action = 'SET'
    ORDER BY action_time DESC LIMIT 1
    """, values_for_check)
    
    # Get the latest action_time
    existing_action_time = cur.fetchone()

    # If an existing action time is found, compare it with the incoming action time
    if existing_action_time:
        latest_action_time = existing_action_time[0]
        # Only proceed if the new action_time is more recent
        if action_time <= latest_action_time:
            print(f"Skipping outdated SET operation for {row_dict} with action_time {action_time}")
            cur.close()
            conn.close()
            return 
    
    # Proceed with the insert or update if the operation is valid (not outdated)
    update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in cols])
    insert_sql = f"""
    INSERT INTO {table_name} ({','.join(cols)})
    VALUES ({','.join(['%s'] * len(values))})
    ON CONFLICT ({','.join(pks)}) DO UPDATE SET
    {update_set};
    """
    
    # Execute the insert or update query
    cur.execute(insert_sql, values)

    # Log the operation only if it is a valid update (not outdated)
    log_sql = f"""
    INSERT INTO {table_name}_log ({','.join(cols)}, action, action_time)
    VALUES ({','.join(['%s'] * len(values))}, %s, %s)
    """
    cur.execute(log_sql, values + ['SET', action_time])

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
