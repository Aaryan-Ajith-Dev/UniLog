from mongo.mongo_service import MongoService
from postgresql.sql_manager import SQL
import time
def main():
    mongo_system=MongoService()
    table = "students"
    mongo_system.load_data(table_name=table)
    sql_system=SQL("students")
    current_time=1000002
    mongo_system.set_item({"student_id":"SID1033","course_id":"CSE016"},{'grade': 'A'},table=table, timestamp=current_time)
    mongo_system.set_item({"student_id":"SID1034","course_id":"CS101"},{'grade': 'A'},table=table, timestamp=1745745394)
    op_log=mongo_system.get_oplog()
    print(op_log)
    sql_system.merge(op_log)
    # sql_system.show_table("students")
    sql_system.show_log_table()

if __name__=="__main__":
    main()