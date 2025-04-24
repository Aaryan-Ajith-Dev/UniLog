import csv
import os
from pyhive import hive
import pandas as pd


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
                    
                    parts = line.split(',', 1)
                    if len(parts) < 2:
                        continue
                    
                    timestamp = int(parts[0].strip())
                    operation = parts[1].strip()
                    
                    # Only process SET operations for timestamp cache
                    if operation.startswith("SET"):
                        parsed_op = self.parse_operation(operation)
                        if parsed_op and parsed_op[0] == "SET":
                            _, student_id, course_id, _ = parsed_op
                            key = (student_id, course_id)
                            self.timestamp_cache[key] = timestamp
        except Exception as e:
            print(f"Error rebuilding timestamp cache: {e}")
    
    def load_data_from_csv(self, csv_file):
        """
        Load data from CSV file into Hive table
        
        Args:
            csv_file (str): Path to the CSV file
        """
        try:
            self.connect()
            
            # Read CSV file to understand its structure
            df = pd.read_csv(csv_file)
            
            # Create table if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS student_course_grades (
                student_id STRING,
                course_id STRING,
                grade STRING,
                PRIMARY KEY (student_id, course_id)
            )
            STORED AS TEXTFILE
            """
            
            self.cursor.execute("DROP TABLE IF EXISTS student_course_grades")
            self.cursor.execute(create_table_query)
            
            # Load data using Hive insertions
            for _, row in df.iterrows():
                # Assuming the CSV has columns for student_id, course_id, grade
                student_id = str(row[0])  # Convert to string to ensure consistency
                course_id = str(row[1])
                grade = str(row[2])
                
                insert_query = f"""
                INSERT INTO student_course_grades 
                VALUES ('{student_id}', '{course_id}', '{grade}')
                """
                self.cursor.execute(insert_query)
                
                # Initial load doesn't generate log entries but we set a timestamp of 0
                key = (student_id, course_id)
                self.timestamp_cache[key] = 0
            
            print(f"Successfully loaded data from {csv_file} into Hive table 'student_course_grades'")
        except Exception as e:
            print(f"Error loading data: {e}")
        finally:
            self.disconnect()
    
    def get(self, student_id, course_id, timestamp=None):
        """
        Get the grade for a specific student_id and course_id
        
        Args:
            student_id (str): Student ID
            course_id (str): Course ID
            timestamp (int, optional): Operation timestamp
            
        Returns:
            str: Grade value if found, None otherwise
        """
        try:
            self.connect()
            query = f"""
            SELECT grade FROM student_course_grades 
            WHERE student_id = '{student_id}' AND course_id = '{course_id}'
            """
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            
            # Log the operation with the provided timestamp
            if timestamp is not None:
                operation = f"GET ({student_id}, {course_id})"
                self._log_operation(operation, timestamp)
            
            if result:
                grade = result[0]
                print(f"Grade for student {student_id} in course {course_id}: {grade}")
                return grade
            else:
                print(f"No grade found for student {student_id} in course {course_id}")
                return None
                
        except Exception as e:
            print(f"Error retrieving data: {e}")
            return None
        finally:
            self.disconnect()
    
    def set(self, student_id, course_id, grade, timestamp=None, log_operation=True):
        """
        Set or update the grade for a specific student_id and course_id
        
        Args:
            student_id (str): Student ID
            course_id (str): Course ID
            grade (str): New grade value
            timestamp (int, optional): Operation timestamp
            log_operation (bool): Whether to log this operation
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.connect()
            
            # Check if the record exists
            query = f"""
            SELECT COUNT(*) FROM student_course_grades 
            WHERE student_id = '{student_id}' AND course_id = '{course_id}'
            """
            self.cursor.execute(query)
            count = self.cursor.fetchone()[0]
            
            if count > 0:
                # Update existing record
                update_query = f"""
                UPDATE student_course_grades 
                SET grade = '{grade}' 
                WHERE student_id = '{student_id}' AND course_id = '{course_id}'
                """
                self.cursor.execute(update_query)
            else:
                # Insert new record
                insert_query = f"""
                INSERT INTO student_course_grades 
                VALUES ('{student_id}', '{course_id}', '{grade}')
                """
                self.cursor.execute(insert_query)
            
            # Log the operation if requested
            if log_operation:
                operation = f"SET (({student_id}, {course_id}), {grade})"
                self._log_operation(operation, timestamp)
                
                # Update timestamp cache
                key = (student_id, course_id)
                if timestamp is not None:
                    self.timestamp_cache[key] = timestamp
                
            print(f"Successfully set grade '{grade}' for student {student_id} in course {course_id}")
            return True
        
        except Exception as e:
            print(f"Error setting data: {e}")
            return False
        finally:
            self.disconnect()
    
    def _log_operation(self, operation, timestamp=None):
        """
        Append an operation to the log file
        
        Args:
            operation (str): The operation to log
            timestamp (int, optional): The timestamp to use
            
        Returns:
            int: The timestamp used
        """
        # If no timestamp provided, generate one
        if timestamp is None:
            # Get the highest timestamp from the log and increment by 1
            try:
                with open(self.oplog_file, 'r') as f:
                    lines = f.readlines()
                    if lines:
                        timestamps = [int(line.split(',')[0].strip()) for line in lines if line.strip()]
                        timestamp = max(timestamps) + 1 if timestamps else 1
                    else:
                        timestamp = 1
            except Exception:
                timestamp = 1
        
        # Write to the log file
        with open(self.oplog_file, 'a') as f:
            f.write(f"{timestamp} , {operation}\n")
            
        return timestamp
    
    def read_oplog(self, system_name):
        """
        Read the operation log of another system
        
        Args:
            system_name (str): Name of the system (e.g., SQL, MONGO)
            
        Returns:
            list: List of operations from the specified system's oplog
        """
        oplog_file = f"oplog.{system_name.lower()}"
        operations = []
        
        try:
            with open(oplog_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        parts = line.split(',', 1)
                        if len(parts) == 2:
                            timestamp = int(parts[0].strip())
                            operation = parts[1].strip()
                            operations.append((timestamp, operation))
            return operations
        except Exception as e:
            print(f"Error reading oplog for {system_name}: {e}")
            return []
    
    def parse_operation(self, operation):
        """
        Parse an operation string into its components
        
        Args:
            operation (str): Operation string (e.g., "SET ((SID103, CSE016), A)")
            
        Returns:
            tuple: (operation_type, student_id, course_id, grade)
        """
        operation = operation.strip()
        
        if operation.startswith("SET"):
            # Extract parameters from SET operation
            # Format: SET ((student_id, course_id), grade)
            try:
                # Remove the SET part
                params = operation[3:].strip()
                
                # Extract the ((student_id, course_id), grade) part
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
                
                # Extract student_id and course_id from (student_id, course_id)
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
    
    def process_command(self, command):
        """
        Process a command from the test case input
        
        Args:
            command (str): Command to process
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            command = command.strip()
            
            # Check if it's a merge command (no timestamp)
            if command.startswith("HIVE.MERGE"):
                # Extract the system name from HIVE.MERGE(SYSTEM)
                system_name = command.split("(")[1].split(")")[0].strip()
                return self.merge(system_name)
            
            # Otherwise, it's a SET or GET command with a timestamp
            parts = command.split(",", 1)
            if len(parts) < 2:
                print(f"Invalid command format: {command}")
                return False
            
            timestamp = int(parts[0].strip())
            operation = parts[1].strip()
            
            # Check if it's a HIVE operation
            if operation.startswith("HIVE."):
                operation = operation[5:]  # Remove "HIVE."
                
                if operation.startswith("SET"):
                    # Extract parameters from SET operation
                    parsed_op = self.parse_operation(operation)
                    if parsed_op and parsed_op[0] == "SET":
                        _, student_id, course_id, grade = parsed_op
                        return self.set(student_id, course_id, grade, timestamp=timestamp)
                
                elif operation.startswith("GET"):
                    # Extract parameters from GET operation
                    parsed_op = self.parse_operation(operation)
                    if parsed_op and parsed_op[0] == "GET":
                        _, student_id, course_id, _ = parsed_op
                        self.get(student_id, course_id, timestamp=timestamp)
                        return True
            
            # Not a HIVE operation or not valid
            return False
            
        except Exception as e:
            print(f"Error processing command: {command} - {e}")
            return False


# Example usage
if __name__ == "__main__":
    # Create a Hive system instance
    hive_system = HiveSystem()
    
    # Load data from CSV
    hive_system.load_data_from_csv("..\dataset\cleaned_grades.csv")
    
    # Process commands from a test case file
    test_file = "testcase.in"
    try:
        with open(test_file, 'r') as f:
            commands = f.readlines()
            
        for command in commands:
            command = command.strip()
            if command:
                print(f"Processing command: {command}")
                hive_system.process_command(command)
                
    except Exception as e:
        print(f"Error processing test case file: {e}")