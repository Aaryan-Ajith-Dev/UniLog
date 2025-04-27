from .db import get_connection
from .schema_utils import get_primary_keys, get_table_schema
from .operations import set_row, get_row
from .merger import merge_log_operations
from .log_table_manager import create_log_table
from .create_database import create_table

class SQL:
    def __init__(self, table_name):
        self.table_name = table_name
        self.conn = get_connection()

    def create_table(self, csv_path):
        """Create the table and insert CSV data."""
        create_table(self.table_name, csv_path)

    def create_log_table(self):
        """Create log table for the specified table."""
        create_log_table(self.table_name)

    def set(self, keys, item, action_time):
        """Perform a SET operation (insert/update) and log it."""
        full_row = {**keys, **item}
        set_row(self.table_name, full_row, action_time)

    def get(self, keys, action_time):
        """Perform a GET operation and log it."""
        return get_row(self.table_name, keys, action_time)

    def merge(self, external_logs):
        """Merge SET operations from external log entries."""
        merge_log_operations(external_logs)

    def show_table(self, table_name=None):
        """Prints the contents of the specified table."""
        if table_name is None:
            table_name = self.table_name

        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM {table_name}")
        rows = cur.fetchall()

        for row in rows:
            print(row)

        cur.close()

    def show_log_table(self):
        """Prints the contents of the log table for the specified table."""
        log_table_name = f"{self.table_name}_log"
        
        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM {log_table_name}")
        rows = cur.fetchall()

        print(f"Contents of the log table ({log_table_name}):")
        for row in rows:
            print(row)

        cur.close()
    def get_log(self):
        """Returns the log table records in a structured format for merging."""
        log_table_name = f"{self.table_name}_log"

        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM {log_table_name}")
        rows = cur.fetchall()

        colnames = [desc[0] for desc in cur.description]
        primary_keys = get_primary_keys(self.table_name)

        logs = []
        for row in rows:
            record = dict(zip(colnames, row))

            # Separate into keys and item
            keys = {k: record[k] for k in primary_keys if k in record}
            item = {k: record[k] for k in record if k not in primary_keys and k not in ['action', 'action_time']}

            structured_log = {
                'timestamp': record['action_time'],
                'operation': record['action'],
                'table': self.table_name,
                'keys': keys,
                'item': item
            }
            logs.append(structured_log)

        cur.close()
        return logs

    def close(self):
        """Close the database connection."""
        self.conn.close()
