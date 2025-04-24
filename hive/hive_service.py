import csv
import os
from pyhive import hive
import pandas as pd
import re

class HiveSystem:
    def __init__(self, host='localhost', port=10000, database='default'):
        """
        Initialize the Hive system with connection parameters and set up the operation log.
        
        Args:
            host (str): Hive server host
            port (int): Hive server port
            database (str): Database name
        """
        self.host = host
        self.port = port
        self.database = database
        self.connection = None
        self.cursor = None
        self.oplog_file = "oplog.hiveql"

        # In-memory cache of timestamps for each (student_id, course_id) pair
        self.timestamp_cache = {}

        # Initialize table-related attributes
        self.table_name = None
        self.all_columns = []

        # Initialize operation log if it doesn't exist
        if not os.path.exists(self.oplog_file):
            with open(self.oplog_file, 'w') as f:
                pass  # Create an empty file

        # Load timestamp cache from existing oplog
        self._rebuild_timestamp_cache()


        
    def connect(self):
        """Establish connection to the Hive server"""
        try:
            self.connection = hive.Connection(
                host=self.host,
                port=self.port,
                database=self.database,
                username='',  # Add username if required
            )
            self.cursor = self.connection.cursor()
            print("Connected to Hive successfully")
        except Exception as e:
            print(f"Error connecting to Hive: {e}")
            raise
    
    def disconnect(self):
        """Close the connection to Hive"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Disconnected from Hive")
    
    def _rebuild_timestamp_cache(self):
        """
        Rebuild the timestamp cache from the existing operation log
        """
        try:
            with open(self.oplog_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    # Split timestamp and operation
                    parts = line.split(',', 1)
                    if len(parts) < 2:
                        continue

                    timestamp = int(parts[0].strip())
                    operation = parts[1].strip()

                    # Process the operation using the generic operation parser
                    if operation.startswith("HIVE."):
                        parsed_op = self.parse_generic_hive_op(operation[5:])
                        if parsed_op:
                            op_type, key_tuple, _ = parsed_op  # Ignore values, we're only interested in the keys

                            # For SET or GET operations, we store the timestamp for the key tuple(s)
                            if op_type in ["SET", "GET"]:
                                for key in key_tuple:
                                    self.timestamp_cache[key] = timestamp
        except Exception as e:
            print(f"Error rebuilding timestamp cache: {e}")

    
    def load_data_from_csv(self, csv_file, loaded=False):
        """
        Load data from CSV file into Hive table, adding a default timestamp of 0 for each row.
        """
        if loaded:
            print("Data already loaded. Skipping...")
            return
        try:
            self.connect()

            self.cursor.execute("SET hive.auto.convert.join=false;")
            self.cursor.execute("SET hive.exec.mode.local.auto=true;")


            # Drop tables if they exist
            self.cursor.execute("DROP TABLE IF EXISTS student_course_grades")
            self.cursor.execute("DROP TABLE IF EXISTS student_course_grades_staging")

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
            self.cursor.execute(create_table_query)

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
            self.cursor.execute(create_staging_table_query)

            # Load data into staging table
            load_data_query = f"""
            LOAD DATA LOCAL INPATH '{csv_file}'
            INTO TABLE student_course_grades_staging
            """
            self.cursor.execute(load_data_query)

            # Insert into final table with custom_timestamp = 0
            insert_query = """
            INSERT INTO TABLE student_course_grades
            SELECT student_id, course_id, roll_no, email_id, grade, 0
            FROM student_course_grades_staging
            """
            self.cursor.execute(insert_query)

            # Drop staging table
            # self.cursor.execute("DROP TABLE IF EXISTS student_course_grades_staging")

            print(f"✅ Successfully loaded data from {csv_file} into Hive table 'student_course_grades'")

        except Exception as e:
            print(f"❌ Error loading data: {e}")

        finally:
            self.disconnect()



    def set_table(self, table_name):
        """
        Set the table name and fetch its schema (column names).
        
        Args:
            table_name (str): Name of the table to work with
        """
        try:
            self.connect()
            self.table_name = table_name

            # Fetch column names
            self.cursor.execute(f"SELECT * FROM {self.table_name} LIMIT 1")
            self.all_columns = [desc[0] for desc in self.cursor.description]

            print(f"Table '{self.table_name}' set with columns: {self.all_columns}")

        except Exception as e:
            print(f"Error setting table: {e}")
            raise
        finally:
            self.disconnect()



    
    def get(self, key_tuple, timestamp=None):
        """
        Generic GET operation for composite keys.

        Args:
            key_tuple (tuple): Composite key as tuple of values (e.g., (student_id, course_id, ...))
            timestamp (int, optional): Operation timestamp

        Returns:
            list: Retrieved values (rows) if found, [] otherwise
        """
        try:
            self.connect()

            if not hasattr(self, "all_columns") or not hasattr(self, "table_name"):
                raise AttributeError("Table schema not set. Call set_table(table_name) first.")

            # Extract key_columns dynamically
            key_columns = self.all_columns[:len(key_tuple)]

            # Build dynamic WHERE clause based on key_columns and key_tuple values
            where_conditions = " AND ".join(
                f"{col} = '{value}'" for col, value in zip(key_columns, key_tuple)
            )

            query = f"SELECT * FROM {self.table_name} WHERE {where_conditions}"
            print(query)
            self.cursor.execute(query)
            result = self.cursor.fetchall()

            # Log the GET operation
            if timestamp is not None:
                key_str = ", ".join(key_tuple)
                operation = f"GET ({key_str})"
                self._log_operation(operation, timestamp)

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
        finally:
            self.disconnect()

    

            
    def set(self, key_tuple, value, set_attr, timestamp=None, log_operation=True):
        """
        Set or insert value for a specific attribute of a composite key with a timestamp.
        Uses INSERT only; no UPDATE. Reuses existing values if row exists.

        Args:
            key_tuple (tuple): Composite key (e.g., (student_id, course_id))
            value: Value to set for the set_attr
            set_attr (str): Column to update/set
            timestamp (int, optional): Operation timestamp
            log_operation (bool): Whether to log this operation
        """
        try:
            self.connect()

            if not hasattr(self, "all_columns") or not hasattr(self, "table_name"):
                raise AttributeError("Table schema not set. Call set_table(table_name) first.")

            key_columns = self.all_columns[:len(key_tuple)]
            value_columns = self.all_columns[len(key_tuple):]

            # Strip table name prefix if present
            key_columns = [col.split('.')[-1] for col in key_columns]
            value_columns = [col.split('.')[-1] for col in value_columns]
            all_columns = key_columns + value_columns + ['custom_timestamp']

            if timestamp is None:
                timestamp = 0

            # Build WHERE clause
            where_clause = " AND ".join(
                f"{col} = '{val}'" for col, val in zip(key_columns, key_tuple)
            )

            # Check for latest row for that key
            query = f"""
            SELECT * FROM {self.table_name}
            WHERE {where_clause}
            ORDER BY custom_timestamp DESC
            LIMIT 1
            """
            self.cursor.execute(query)
            existing_row = self.cursor.fetchone()

            # Start building value dictionary
            all_values_dict = dict(zip(key_columns, key_tuple))
            for col in value_columns:
                all_values_dict[col] = None  # Default to NULL

            if existing_row:
                for col, val in zip(all_columns, existing_row):
                    if col not in key_columns and col != 'custom_timestamp':
                        all_values_dict[col] = val

            # Override with new value for the target column
            if isinstance(value, tuple) and len(value) == 1:
                value = value[0]
            all_values_dict[set_attr] = value
            all_values_dict['custom_timestamp'] = timestamp

            insert_columns = all_values_dict.keys()
            insert_values = all_values_dict.values()

            insert_query = f"""
            INSERT INTO {self.table_name} ({", ".join(insert_columns)}) 
            VALUES ({", ".join("NULL" if v is None else f"'{v}'" for v in insert_values)})
            """
            print(f"Executing query: {insert_query}")
            self.cursor.execute(insert_query)

            if log_operation:
                key_str = ", ".join(key_tuple)
                val_str = str(value)
                operation = f"SET (({key_str}), {val_str})"
                self._log_operation(operation, timestamp)
                self.timestamp_cache[key_tuple] = timestamp

            print(f"✅ Successfully set {set_attr} = {value} for key {key_tuple} with timestamp {timestamp}")
            return True

        except Exception as e:
            print(f"❌ Error setting data: {e}")
            return False
        finally:
            self.disconnect()






        
    def _log_operation(self, operation, timestamp=None):
        """
        Append an operation to the operation log with a timestamp.

        Args:
            operation (str): The operation string (e.g., SET ((k1, k2), v1, v2))
            timestamp (int, optional): The timestamp to use. If None, auto-generate.

        Returns:
            int: The timestamp used
        """
        if timestamp is None:
            try:
                with open(self.oplog_file, 'r') as f:
                    lines = [line for line in f if line.strip()]
                    if lines:
                        timestamps = [int(line.split(',')[0].strip()) for line in lines]
                        timestamp = max(timestamps) + 1
                    else:
                        timestamp = 1
            except Exception:
                timestamp = 1

        # Write the operation with the timestamp
        with open(self.oplog_file, 'a') as f:
            f.write(f"{timestamp} , {operation}\n")

        return timestamp

    
    def read_oplog(self, system_name):
        """
        Read the operation log of another system.

        Args:
            system_name (str): Name of the system (e.g., SQL, MONGO)

        Returns:
            list: List of (timestamp, operation) tuples
        """
        oplog_file = f"oplog.{system_name.lower()}"
        operations = []

        try:
            with open(oplog_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    # Find the first comma that separates timestamp and operation
                    comma_index = line.find(',')
                    if comma_index == -1:
                        continue

                    timestamp_part = line[:comma_index].strip()
                    operation_part = line[comma_index + 1:].strip()

                    try:
                        timestamp = int(timestamp_part)
                        operations.append((timestamp, operation_part))
                    except ValueError:
                        print(f"Invalid timestamp format in line: {line}")
                        continue

            return operations

        except Exception as e:
            print(f"Error reading oplog for {system_name}: {e}")
            return []

    
    def parse_operation(self, operation):
        """
        Parse an operation string into its components
        
        Args:
            operation (str): Operation string
            
        Returns:
            tuple: (operation_type, student_id, course_id, grade)
        """
        operation = operation.strip()
        
        if operation.startswith("SET"):
            # Extract parameters from SET operation
            try:
                # Remove the SET part
                params = operation[3:].strip()
           
                if params.startswith("(") and params.endswith(")"):
                    params = params[1:-1].strip()
                
                # Split into two parts: (student_id, course_id) and grade
                parts = params.split(",")
                
                # Find the closing parenthesis for the first part
                paren_count = 0
                split_index = 0
                for i, char in enumerate(params):
                    if char == '(':
                        paren_count += 1
                    elif char == ')':
                        paren_count -= 1
                        if paren_count == 0:
                            split_index = i + 1
                            break
                
                id_part = params[:split_index].strip()
                grade = params[split_index:].strip()
                if grade.startswith(","):
                    grade = grade[1:].strip()
                
                if id_part.startswith("(") and id_part.endswith(")"):
                    id_part = id_part[1:-1].strip()
                
                id_parts = id_part.split(",")
                student_id = id_parts[0].strip()
                course_id = id_parts[1].strip()
                
                return ("SET", student_id, course_id, grade)
            except Exception as e:
                print(f"Error parsing SET operation: {operation} - {e}")
                return None
            
        elif operation.startswith("GET"):
            # Extract parameters from GET operation
            try:
                # Remove the GET part and parentheses
                params = operation[3:].strip()
                if params.startswith("(") and params.endswith(")"):
                    params = params[1:-1].strip()
                
                # Split into student_id and course_id
                parts = params.split(",")
                student_id = parts[0].strip()
                course_id = parts[1].strip() if len(parts) > 1 else ""
                
                return ("GET", student_id, course_id, None)
            except Exception as e:
                print(f"Error parsing GET operation: {operation} - {e}")
                return None
        
        return None
    
    def merge(self, system_name):
        """
        Merge the state with another system based on its operation log,
        applying SET operations if the other system's timestamp is newer.
        
        Args:
            system_name (str): Name of the system to merge with (e.g., SQL, MONGO)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            print(f"Starting merge with {system_name}...")
            operations = self.read_oplog(system_name)
            
            # Process only SET operations and consider timestamps
            for timestamp, operation in operations:
                parsed_op = self.parse_operation(operation)
                if parsed_op is None or parsed_op[0] != "SET":
                    continue
                
                _, student_id, course_id, grade = parsed_op
                key = (student_id, course_id)
                
                # Check if we should apply this operation based on timestamp
                should_apply = False
                
                # If we don't have this record or our version is older, apply the update
                if key not in self.timestamp_cache or self.timestamp_cache[key] < timestamp:
                    should_apply = True
                    print(f"Applying {system_name} operation: {operation} (timestamp: {timestamp})")
                    
                    # Apply the SET operation but don't log it (we're just synchronizing data)
                    self.set(student_id, course_id, grade, timestamp=timestamp, log_operation=False)
                    
                    # Update our timestamp cache with the other system's timestamp
                    self.timestamp_cache[key] = timestamp
                else:
                    print(f"Skipping {system_name} operation: {operation} (our timestamp: {self.timestamp_cache[key]} > {timestamp})")
            
            print(f"Successfully merged with {system_name}")
            return True
            
        except Exception as e:
            print(f"Error merging with {system_name}: {e}")
            return False
    



    def parse_generic_hive_op(self, operation_str):
        """
        Parses generic HIVE.SET or HIVE.GET operation.

        - SET ((k1, k2, ..., kn), v1, v2, ..., vm)
        - GET (k1, k2, ..., kn)

        Returns:
            ("SET" or "GET", tuple of keys, tuple of values or None)
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
    
    def process_command(self, command, set_attr):
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
                parsed = self.parse_generic_hive_op(operation[5:])
                if not parsed:
                    print(f"Failed to parse HIVE operation: {operation}")
                    return False

                op_type, key_tuple, value_tuple = parsed
                print(f"Parsed operation: {op_type}, keys: {key_tuple}, values: {value_tuple}, timestamp: {timestamp}")

                if op_type == "SET":
                    return self.set(key_tuple, value_tuple,set_attr, timestamp=timestamp)
                elif op_type == "GET":
                    self.get(key_tuple, timestamp=timestamp)
                    return True

            return False

        except Exception as e:
            print(f"Error processing command: {command} - {e}")
            return False




if __name__ == "__main__":
    # Create a Hive system instance
    hive_system = HiveSystem()
    
    # Load data from CSV
    hive_system.load_data_from_csv("/home/sohith/Desktop/nosql/project/UniLog/dataset/student_course_grades.csv",False)
    
    # Set the table name and extract schema (this will populate self.table_name and self.all_columns)
    hive_system.set_table("student_course_grades")  # <-- Add this

    # Process commands from a test case file
    test_file = "/home/sohith/Desktop/nosql/project/UniLog/hive/testcase.in"
    set_attr = "grade" 
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
