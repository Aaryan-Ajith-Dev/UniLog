from hive.better_hive_service import HiveSystem
from postgresql.sql_manager import SQL

def main():
    """Main function to run the Hive system"""
    # Create a Hive system instance
    hive_system = HiveSystem()
    sql_system = SQL("student_course_grades")


    key = ["student_id", "course_id"]
    
    try:
        hive_system.connect()
        hive_system.set_table("student_course_grades")
        hive_system.build_timestamp_cache(key)
        print("Connected to Hive and PostgreSQL systems.")

        sql_system.create_table("/home/sohith/Desktop/nosql/project/UniLog/dataset/student_course_grades_head.csv")
        sql_system.create_log_table()
        print("Created tables in PostgreSQL.")
        
        elog = hive_system.oplog_manager.get_oplog()
        print("Oplog fetched successfully.")

        status = sql_system.merge(elog)
        sql_system.show_table()
        sql_system.show_log_table()

            
    except Exception as e:
        print(f"System error: {e}")
    finally:
        hive_system.disconnect()


if __name__ == "__main__":
    main()

