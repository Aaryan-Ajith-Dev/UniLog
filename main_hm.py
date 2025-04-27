from mongo.mongo_service import MongoService
from hive.better_hive_service import HiveSystem
from postgresql.sql_manager import SQL
import re


def parse_generic_op(operation_str: str):
    """
    Parse a generic SET or GET operation across HIVE, SQL, or MONGO.

    Args:
        operation_str (str): Operation string

    Returns:
        tuple: (system, operation type, keys, values)
    """
    operation_str = operation_str.strip()

    # Match system and actual operation
    system_match = re.match(r"(HIVE|SQL|MONGO)\.(SET|GET)\s*(.*)", operation_str, re.IGNORECASE)
    if not system_match:
        return None

    system = system_match.group(1).upper()
    op_type = system_match.group(2).upper()
    op_content = system_match.group(3).strip()

    if op_type == "SET":
        # Match SET ((k1, k2, ...), v1, v2, ..., vm)
        set_match = re.match(r"\(\(\s*(.*?)\s*\)\s*,\s*(.*)\)", op_content)
        if not set_match:
            return None
        keys = tuple(part.strip() for part in set_match.group(1).split(","))
        values = tuple(part.strip() for part in set_match.group(2).split(","))
        return (system, op_type, keys, values)

    elif op_type == "GET":
        # Match GET (k1, k2, ...)
        get_match = re.match(r"\(\s*(.*?)\s*\)", op_content)
        if not get_match:
            return None
        keys = tuple(part.strip() for part in get_match.group(1).split(","))
        return (system, op_type, keys, None)

    return None

def process_command(command: str, set_attr: list, systems,key):
    """
    Process a command string from external source.

    Args:
        command (str): Command string
        set_attr (list): Attributes to set
        set_func (function): Function to handle set
        get_func (function): Function to handle get
        merge_func (function): Function to handle merge

    Returns:
        bool: Success status
    """
    try:
        command = command.strip()

        # Handle MERGE commands
        merge_match = re.match(r"(HIVE|SQL|MONGO)\.MERGE\s*\(\s*(HIVE|SQL|MONGO)\s*\)", command, re.IGNORECASE)
        if merge_match:
            system_get = merge_match.group(1).upper()
            system_give = merge_match.group(2).upper()
            systems[system_get].merge(system_give, systems[system_give].get_oplog())
            return True
                

        # Handle normal timestamped operations
        parts = command.split(",", 1)
        if len(parts) < 2:
            print(f"Invalid command format: {command}")
            return False

        timestamp = int(parts[0].strip())
        operation = parts[1].strip()

        parsed = parse_generic_op(operation)
        if not parsed:
            print(f"Failed to parse operation: {operation}")
            return False

        system, op_type, key_tuple, value_tuple = parsed
        print(f"Parsed operation: {system}, {op_type}, keys: {key_tuple}, values: {value_tuple}, timestamp: {timestamp}")

        if op_type == "SET":
            if system == "HIVE":
                systems[system].set(key_tuple, value_tuple, set_attr, timestamp=timestamp)
            
            if system == "SQL":
                print({key: value for key, value in zip(key, key_tuple)},
                    {key: value for key, value in zip(set_attr, value_tuple)})
                systems[system].set({key: value for key, value in zip(key, key_tuple)},
                    {key: value for key, value in zip(set_attr, value_tuple)},timestamp)
            
            if system == "MONGO":
                systems[system].set_item(
                    {key: value for key, value in zip(key, key_tuple)},
                    {key: value for key, value in zip(set_attr, value_tuple)},
                    table="student_course_grades",
                    timestamp=timestamp
                )


        elif op_type == "GET":
            if system == "HIVE":
                systems[system].get(key_tuple, timestamp=timestamp)
            
            if system == "SQL":
                print({key: value for key, value in zip(key, key_tuple)})
                systems[system].get({key: value for key, value in zip(key, key_tuple)},timestamp)

            if system == "MONGO":
                systems[system].get_item(
                    {key: value for key, value in zip(key, key_tuple)},
                    timestamp=timestamp,
                    table="student_course_grades"
                )

        return False

    except Exception as e:
        print(f"Error processing command: {command} - {e}")
        return False



def main():
    """Main function to run the Hive system"""
    # Create a Hive system instance
    recreate_hive = True
    recreate_sql = True
    recreate_mongo = True

    hive_system = HiveSystem()
    mongo_system = MongoService(recreate=recreate_mongo,table="student_course_grades")
    sql_system = SQL("student_course_grades")

    systems = {
        "HIVE": hive_system,
        "SQL": sql_system,
        "MONGO": mongo_system
    }

    key = ["student_id", "course_id"]
    set_attr = ["grade"]
    
    
    try:
        hive_system.connect()
        csv_path = "/home/sohith/Desktop/nosql/project/UniLog/dataset/student_course_grades.csv"
        status = hive_system.load_data_from_csv(csv_path, recreate_hive)
        print("Loading status",status)
        hive_system.set_table("student_course_grades")
        hive_system.create_oplog_table(recreate_hive)
        hive_system.build_timestamp_cache(key)


        mongo_system.load_data()

        
        sql_system.create_table("/home/sohith/Desktop/nosql/project/UniLog/dataset/student_course_grades_head.csv",recreate_sql)
        sql_system.create_log_table(recreate_sql)
        print("Connected to Hive,MongoDB and PostgreSQL systems.")

        test_file = "/home/sohith/Desktop/nosql/project/UniLog/testcase.in"
       
        with open(test_file, 'r') as f:
            commands = f.readlines()
            
        for command in commands:
            command = command.strip()
            if command:
                print(f"Processing command: {command}")
                process_command(command, set_attr,systems,key)   

            
    except Exception as e:
        print(f"System error: {e}")
    finally:
        hive_system.disconnect()
        mongo_system.close()


if __name__ == "__main__":
    main()

