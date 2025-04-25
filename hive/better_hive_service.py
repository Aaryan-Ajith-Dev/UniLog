import csv
import os
import re
import ast
import tempfile
import pandas as pd
from pyhive import hive
from typing import List, Dict, Tuple, Optional, Any, Union

class HiveConnection:
    """
    Class to manage Hive database connections.
    """
    def __init__(self, host='localhost', port=10000, database='default', username=''):
        """
        Initialize connection parameters for Hive.
        
        Args:
            host (str): Hive server host
            port (int): Hive server port
            database (str): Database name
            username (str): Username for authentication
        """
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """Establish connection to the Hive server"""
        try:
            self.connection = hive.Connection(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username
            )
            self.cursor = self.connection.cursor()
            print("Connected to Hive successfully")
            return True
        except Exception as e:
            print(f"Error connecting to Hive: {e}")
            raise
    
    def disconnect(self):
        """Close the connection to Hive"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            self.cursor = None
            self.connection = None
        print("Disconnected from Hive")
        
    def execute(self, query):
        """
        Execute a query on Hive.
        
        Args:
            query (str): The SQL query to execute
            
        Returns:
            Any: Query results if applicable
        """
        try:
            print(f"Executing query: {query}")
            self.cursor.execute(query)
            return True
        except Exception as e:
            print(f"Error executing query: {e}")
            return False
            
    def fetch_one(self):
        """Fetch one row from the result set"""
        return self.cursor.fetchone() if self.cursor else None
        
    def fetch_all(self):
        """Fetch all rows from the result set"""
        return self.cursor.fetchall() if self.cursor else []
        
    def get_description(self):
        """Get column descriptions from the last query"""
        return self.cursor.description if self.cursor else None


class TimestampCache:
    """
    Class to manage timestamp caching for keys.
    """
    def __init__(self):
        """Initialize an empty timestamp cache."""
        self.cache = {}
        
    def get(self, key_tuple: tuple, default=-1) -> int:
        """
        Get the timestamp for a key.
        
        Args:
            key_tuple (tuple): The composite key
            default (int): Default value if key not found
            
        Returns:
            int: The timestamp value
        """
        return self.cache.get(key_tuple, default)
        
    def set(self, key_tuple: tuple, timestamp: int) -> None:
        """
        Set the timestamp for a key.
        
        Args:
            key_tuple (tuple): The composite key
            timestamp (int): The timestamp value to set
        """
        self.cache[key_tuple] = timestamp
        print(f"Assigned timestamp for key {key_tuple} with value {timestamp}.")
        
    def build_from_query(self, conn, table_name: str, prime_attr: List[str]) -> None:
        """
        Build cache from database query results.
        
        Args:
            conn: HiveConnection instance
            table_name (str): Table to query
            prime_attr (list): List of key attribute names
        """
        try:
            group_by_cols = ", ".join(prime_attr + ['custom_timestamp'])
            query = f"SELECT {group_by_cols} FROM {table_name}"
            
            print(f"Executing timestamp cache init query: {query}")
            conn.execute(query)
            results = conn.fetch_all()
            
            print(results)
            
            for row in results:
                key = tuple(str(val) for val in row[:-1])
                time_stamp = row[-1]
                val = self.get(key, -1)
                if time_stamp > val:
                    self.set(key, time_stamp)
                    
            print("✅ Timestamp cache initialized with dumped data.")
            print(f"Cached {len(self.cache)} prime key combinations.")
            
        except Exception as e:
            print(f"❌ Failed to build timestamp cache: {e}")


class OplogManager:
    """
    Class to manage operation logging.
    """
    def __init__(self, conn):
        """
        Initialize the operation log manager.
        
        Args:
            conn: HiveConnection instance
        """
        self.conn = conn

        
    def create_oplog_table(self, recreate=False) -> bool:
        """
        Creates the oplog table in Hive.
        
        Args:
            recreate (bool): Whether to drop and recreate the table
        
        Returns:
            bool: Success status
        """
        try:
            if recreate:
                self.conn.execute("DROP TABLE IF EXISTS oplog")
                print("Dropped existing oplog table.")

            create_table_query = """
            CREATE TABLE IF NOT EXISTS oplog (
                custom_timestamp INT,
                operation STRING,
                table_name STRING,
                keys ARRAY<STRING>,  -- Use array for keys
                item ARRAY<STRING>   -- Use array for item
            )
            STORED AS TEXTFILE
            LOCATION '/home/sohith/Desktop/nosql/project/UniLog/hive/tmp/oplog/'
            """

            self.conn.execute(create_table_query)
            print("✅ Successfully created the oplog table.")
            return True
            
        except Exception as e:
            print(f"❌ Error creating oplog table: {e}")
            return False
            
    def log_entry(self, operation: str, timestamp: int, table_name: str, 
                 key_tuple: tuple, column_names: List[str], set_attrs=None, values=None) -> bool:
        """
        Log an operation to the oplog table.
        
        Args:
            operation (str): Operation type (SET, GET)
            timestamp (int): Operation timestamp
            table_name (str): Target table name
            key_tuple (tuple): Key values
            column_names (List[str]): Column names for keys
            set_attrs (List[str]): Attributes being set
            values (List): Values being set
            
        Returns:
            bool: Success status
        """
        try:
            # Build the keys array (composite keys)
            keys_array = [f"{key}: {val}" for key, val in zip(column_names[:len(key_tuple)], key_tuple)]

            # Build the item array (set attributes and values)
            if set_attrs is None and values is None:
                item_array = []
            else:
                item_array = [f"{attr}: {val}" for attr, val in zip(set_attrs, values)]

            # Hive insert query
            insert_query = f"""
            INSERT INTO oplog (custom_timestamp, operation, table_name, keys, item)
            VALUES ({timestamp}, '{operation}', '{table_name}', 
                    array({', '.join(f"'{k}'" for k in keys_array)}), 
                    array({', '.join(f"'{i}'" for i in item_array)}))
            """
            
            # Execute the insert query
            self.conn.execute(insert_query)
            print(f"✅ Successfully logged operation to oplog: {keys_array}, {item_array}")
            return True
            
        except Exception as e:
            print(f"❌ Error logging operation: {e}")
            return False
            
    def get_oplog(self) -> List[Dict]:
        """
        Retrieve the oplog data.
        
        Returns:
            List[Dict]: List of operation log entries
        """
        try:
            # Fetch oplog entries
            self.conn.execute("SELECT * FROM oplog")
            rows = self.conn.fetch_all()

            oplog_data = []
            for row in rows:
                timestamp, operation, table_name, keys_str, item_str = row
                
                keys_dict = self._parse_key_value_list(keys_str)
                items_dict = self._parse_key_value_list(item_str)

                entry = {
                    "timestamp": timestamp,
                    "operation": operation,
                    "table": table_name,
                    "keys": keys_dict,
                    "item": items_dict
                }

                oplog_data.append(entry)

            return oplog_data

        except Exception as e:
            print(f"❌ Error fetching oplog data: {e}")
            return []
            
    def _parse_key_value_list(self, kv_list_str):
        """
        Parse a key-value list string into a dictionary.
        
        Args:
            kv_list_str (str): String representation of key-value pairs
            
        Returns:
            dict: Parsed key-value dictionary
        """
        kv_dict = {}
        try:
            kv_list = ast.literal_eval(kv_list_str)
            for pair in kv_list:
                if ":" in pair:
                    key, value = pair.split(":", 1)
                    kv_dict[key.strip()] = value.strip()
        except Exception as e:
            print(f"❌ Error parsing keys/items: {e}")
        return kv_dict


class TableManager:
    """
    Class to manage table operations.
    """
    def __init__(self, conn):
        """
        Initialize table manager.
        
        Args:
            conn: HiveConnection instance
        """
        self.conn = conn
        self.table_name = None
        self.all_columns = []
        
    def set_table(self, table_name: str) -> bool:
        """
        Set the current table and fetch its schema.
        
        Args:
            table_name (str): Name of table to work with
            
        Returns:
            bool: Success status
        """
        try:
            self.table_name = table_name

            # Fetch column names
            self.conn.execute(f"SELECT * FROM {self.table_name} LIMIT 1")
            self.all_columns = [desc[0] for desc in self.conn.get_description()]

            print(f"Table '{self.table_name}' set with columns: {self.all_columns}")
            return True

        except Exception as e:
            print(f"Error setting table: {e}")
            return False
            
    def load_data_from_csv(self, csv_file: str, recreate: bool = False) -> bool:
        """
        Load data from CSV into Hive table.
        
        Args:
            csv_file (str): Path to CSV file
            recreate (bool): Whether to recreate table
            
        Returns:
            bool: Success status
        """
        if not recreate:
            print("Data already loaded. Skipping...")
            return True
            
        try:
            with open(csv_file, 'r') as f:
                lines = f.readlines()
                data_lines = lines[1:]  # skip header
                temp_csv = tempfile.NamedTemporaryFile(delete=False, mode='w')
                temp_csv.writelines(data_lines)
                temp_csv.close()

            self.conn.execute("SET hive.auto.convert.join=false;")
            self.conn.execute("SET hive.exec.mode.local.auto=true;")

            # Drop tables if they exist
            self.conn.execute("DROP TABLE IF EXISTS student_course_grades")
            self.conn.execute("DROP TABLE IF EXISTS student_course_grades_staging")

            # Create final table
            create_table_query = """
            CREATE TABLE IF NOT EXISTS student_course_grades (
                student_id STRING,
                course_id STRING,
                roll_no STRING,
                email_id STRING,
                grade STRING,
                custom_timestamp INT
            )
            STORED AS TEXTFILE
            LOCATION '/home/sohith/Desktop/nosql/project/UniLog/hive/tmp/student_course_grades/'
            """
            self.conn.execute(create_table_query)

            # Create staging table
            create_staging_table_query = """
            CREATE TABLE IF NOT EXISTS student_course_grades_staging (
                student_id STRING,
                course_id STRING,
                roll_no STRING,
                email_id STRING,
                grade STRING
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            WITH SERDEPROPERTIES (
                "separatorChar" = ","
            )
            STORED AS TEXTFILE
            LOCATION '/home/sohith/Desktop/nosql/project/UniLog/hive/tmp/student_course_grades_staging/'
            TBLPROPERTIES ("skip.header.line.count"="1")
            """
            self.conn.execute(create_staging_table_query)

            # Load data into staging table
            load_data_query = f"""
            LOAD DATA LOCAL INPATH '{csv_file}'
            INTO TABLE student_course_grades_staging
            """
            self.conn.execute(load_data_query)

            # Insert into final table with custom_timestamp = 0
            insert_query = """
            INSERT INTO TABLE student_course_grades
            SELECT student_id, course_id, roll_no, email_id, grade, 0
            FROM student_course_grades_staging
            """
            self.conn.execute(insert_query)

            # Drop staging table
            self.conn.execute("DROP TABLE IF EXISTS student_course_grades_staging")

            print(f"✅ Successfully loaded data from {csv_file} into Hive table 'student_course_grades'")
            return True

        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return False


class OperationParser:
    """
    Class to parse different types of operations.
    """
     
    @staticmethod
    def parse_generic_hive_op(operation_str: str):
        """
        Parse generic HIVE.SET or HIVE.GET operation.
        
        Args:
            operation_str (str): Operation string
            
        Returns:
            tuple: Parsed operation components
        """
        operation_str = operation_str.strip()

        # Match SET ((k1, k2, ...), v1, v2, ..., vm)
        set_match = re.match(r"SET\s*\(\(\s*(.*?)\s*\)\s*,\s*(.*)\)", operation_str)
        if set_match:
            keys = tuple(part.strip() for part in set_match.group(1).split(","))
            values = tuple(part.strip() for part in set_match.group(2).split(","))
            return ("SET", keys, values)

        # Match GET (k1, k2, ...)
        get_match = re.match(r"GET\s*\(\s*(.*?)\s*\)", operation_str)
        if get_match:
            keys = tuple(part.strip() for part in get_match.group(1).split(","))
            return ("GET", keys, None)

        return None


class HiveSystem:
    """
    Main system class integrating all components.
    """
    def __init__(self, host='localhost', port=10000, database='default'):
        """
        Initialize the Hive system.
        
        Args:
            host (str): Hive server host
            port (int): Hive server port
            database (str): Database name
        """
        self.connection = HiveConnection(host, port, database)
        self.timestamp_cache = TimestampCache()
        self.oplog_manager = None  # Initialize after connection
        self.table_manager = None  # Initialize after connection
        self.parser = OperationParser()
        
    def connect(self):
        """Connect to Hive and initialize components"""
        self.connection.connect()
        self.oplog_manager = OplogManager(self.connection)
        self.table_manager = TableManager(self.connection)
        
    def disconnect(self):
        """Disconnect from Hive"""
        self.connection.disconnect()
        
    def set_table(self, table_name):
        """Set the active table"""
        return self.table_manager.set_table(table_name)
        
    def load_data_from_csv(self, csv_file, recreate=False):
        """Load data from CSV file"""
        return self.table_manager.load_data_from_csv(csv_file, recreate)
        
    def create_oplog_table(self, recreate=False):
        """Create oplog table"""
        return self.oplog_manager.create_oplog_table(recreate)
        
    def build_timestamp_cache(self, prime_attr):
        """Build timestamp cache from database"""
        self.timestamp_cache.build_from_query(self.connection, self.table_manager.table_name, prime_attr)
    
    def get(self, key_tuple, timestamp=None):
        """
        Execute GET operation.
        
        Args:
            key_tuple (tuple): Composite key
            timestamp (int): Operation timestamp
        
        Returns:
            list: Query results
        """
        try:
            if not self.table_manager.all_columns:
                raise AttributeError("Table schema not set. Call set_table(table_name) first.")

            # Extract key_columns dynamically
            key_columns = self.table_manager.all_columns[:len(key_tuple)]

            # Build dynamic WHERE clause based on key_columns and key_tuple values
            where_conditions = " AND ".join(
                f"{col} = '{value}'" for col, value in zip(key_columns, key_tuple)
            )

            query = f"SELECT * FROM {self.table_manager.table_name} WHERE {where_conditions}"
            self.connection.execute(query)
            result = self.connection.fetch_all()

            # Log the GET operation
            if timestamp is not None:
                self.oplog_manager.log_entry(
                    'GET', 
                    timestamp, 
                    self.table_manager.table_name,
                    key_tuple, 
                    self.table_manager.all_columns
                )

            if result:
                for row in result:
                    print(f"Retrieved: {row}")
                return result
            else:
                print(f"No entry found for key: {key_tuple}")
                return []

        except Exception as e:
            print(f"Error retrieving data: {e}")
            return []
    
    def set(self, key_tuple, values, set_attrs, timestamp=None, log_operation=True):
        """
        Execute SET operation.
        
        Args:
            key_tuple (tuple): Composite key
            values (list): Values to set
            set_attrs (list): Attributes to set
            timestamp (int): Operation timestamp
            log_operation (bool): Whether to log the operation
            
        Returns:
            bool: Success status
        """
        try:
            if not self.table_manager.all_columns:
                raise AttributeError("Table schema not set. Call set_table(table_name) first.")

            key_columns = self.table_manager.all_columns[:len(key_tuple)]
            value_columns = self.table_manager.all_columns[len(key_tuple):]
            key_columns = [col.split('.')[-1] for col in key_columns]
            value_columns = [col.split('.')[-1] for col in value_columns]
            all_columns = key_columns + value_columns + ['custom_timestamp']

            if timestamp is None:
                timestamp = 0

            # Build WHERE clause to check for existing row
            where_clause = " AND ".join(
                f"{col} = '{val}'" for col, val in zip(key_columns, key_tuple)
            )

            query = f"SELECT * FROM {self.table_manager.table_name} WHERE {where_clause}"
            self.connection.execute(query)
            existing_row = self.connection.fetch_one()
            print(existing_row)

            # Initialize all values dict with key values
            all_values_dict = dict(zip(key_columns, key_tuple))

            # Set default NULLs for all value_columns
            for col in value_columns:
                all_values_dict[col] = None

            # If an existing row is found, preserve the existing values for the non-modified columns
            if existing_row:
                for col, val in zip(all_columns, existing_row):
                    if col not in key_columns and col != 'custom_timestamp':
                        all_values_dict[col] = val

                # If timestamp in cache is greater than the one provided, skip
                cache_time = self.timestamp_cache.get(key_tuple, -1)
                
                if timestamp <= cache_time:
                    print(f"⚠️ Skipping update. Timestamp {timestamp} is not newer than the cached timestamp {cache_time} for key {key_tuple}.")
                    return False

            # Set new values for the specified set_attrs
            for attr, val in zip(set_attrs, values):
                all_values_dict[attr] = val
            all_values_dict['custom_timestamp'] = timestamp

            insert_columns = all_values_dict.keys()
            insert_values = all_values_dict.values()

            insert_query = f"""
            INSERT INTO {self.table_manager.table_name} ({", ".join(insert_columns)}) 
            VALUES ({", ".join("NULL" if v is None else f"'{v}'" for v in insert_values)})
            """
            self.connection.execute(insert_query)

            # Log the operation if needed
            if log_operation:
                self.oplog_manager.log_entry(
                    'SET', 
                    timestamp, 
                    self.table_manager.table_name,
                    key_tuple, 
                    self.table_manager.all_columns,
                    set_attrs, 
                    values
                )

            # Update timestamp cache
            self.timestamp_cache.set(key_tuple, timestamp)

            print(f"✅ Successfully set {set_attrs} = {values} for key {key_tuple} with timestamp {timestamp}")
            return True

        except Exception as e:
            print(f"❌ Error setting data: {e}")
            return False
    
    def merge(self, system_name):
        """
        Merge with another system based on oplog.
        
        Args:
            system_name (str): Name of system to merge with
            
        Returns:
            bool: Success status
        """
        try:
            # Example: Load oplog from external source (replace with real loader)
            external_oplog = [{'timestamp': 1, 'operation': 'SET', 'table': 'student_course_grades', 'keys': {'student_id': 'SID103', 'course_id':'CSE016'}, 'item': {'grade': 'B'}}]

            applied_count = 0

            for entry in external_oplog:
                if entry["operation"] != "SET":
                    continue  # Only SET operations affect state

                keys = entry["keys"]
                item = entry["item"]
                timestamp = int(entry["timestamp"])
                table = entry.get("table", self.table_manager.table_name)
                print(f'{keys},{item},{timestamp},{table}')
                
                # Validate target table
                if table != self.table_manager.table_name:
                    continue    

                attribute_names = [col.split('.')[-1] for col in self.table_manager.all_columns[:len(keys)]]
                # Convert keys to tuple for timestamp cache lookup
                key_tuple = tuple(keys[col] for col in attribute_names)
                cache_time = self.timestamp_cache.get(key_tuple, -1)
                print(f'{key_tuple}, {cache_time}')
                
                if timestamp > cache_time:
                    # Apply SET operation
                    set_attrs = list(item.keys())
                    values = list(item.values())
                    
                    success = self.set(key_tuple, values, set_attrs, timestamp=timestamp)
                    if success:
                        applied_count += 1

            print(f"✅ Merge complete. Applied {applied_count} newer SET operations from {system_name}.")
            return True

        except Exception as e:
            print(f"❌ Error merging with {system_name}: {e}")
            return False
    
    def process_command(self, command, set_attr):
        """
        Process a command from external source.
        
        Args:
            command (str): Command string
            set_attr (list): Default attributes to set
            
        Returns:
            bool: Success status
        """
        try:
            command = command.strip()

            # Handle HIVE.MERGE
            if command.startswith("HIVE.MERGE"):
                system_name = command.split("(")[1].split(")")[0].strip()
                return self.merge(system_name)

            # Split timestamp and operation
            parts = command.split(",", 1)
            if len(parts) < 2:
                print(f"Invalid command format: {command}")
                return False

            timestamp = int(parts[0].strip())
            operation = parts[1].strip()

            if operation.startswith("HIVE."):
                parsed = self.parser.parse_generic_hive_op(operation[5:])
                if not parsed:
                    print(f"Failed to parse HIVE operation: {operation}")
                    return False

                op_type, key_tuple, value_tuple = parsed
                print(f"Parsed operation: {op_type}, keys: {key_tuple}, values: {value_tuple}, timestamp: {timestamp}")

                if op_type == "SET":
                    return self.set(key_tuple, value_tuple, set_attr, timestamp=timestamp)
                elif op_type == "GET":
                    self.get(key_tuple, timestamp=timestamp)
                    return True

            return False

        except Exception as e:
            print(f"Error processing command: {command} - {e}")
            return False
    
    def get_oplog(self):
        """Get the operation log"""
        return self.oplog_manager.get_oplog()


def main():
    """Main function to run the Hive system"""
    # Create a Hive system instance
    hive_system = HiveSystem()
    
    try:
        hive_system.connect()
        recreate = False
        key = ["student_id", "course_id"]
        set_attr = ["grade", "roll_no"]

        # Load data from CSV
        csv_path = "/home/sohith/Desktop/nosql/project/UniLog/dataset/student_course_grades.csv"
        hive_system.load_data_from_csv(csv_path, recreate)
        
        # Set the table name and extract schema
        hive_system.set_table("student_course_grades")
        hive_system.create_oplog_table(recreate)
        
        hive_system.build_timestamp_cache(key)

        # Process commands from a test case file
        test_file = "/home/sohith/Desktop/nosql/project/UniLog/hive/testcase.in"
         
        try:
            with open(test_file, 'r') as f:
                commands = f.readlines()
                
            for command in commands:
                command = command.strip()
                if command:
                    print(f"Processing command: {command}")
                    hive_system.process_command(command, set_attr)   
        except Exception as e:
            print(f"Error processing test case file: {e}")
            
    except Exception as e:
        print(f"System error: {e}")
    finally:
        hive_system.disconnect()


if __name__ == "__main__":
    main()