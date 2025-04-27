from postgresql.sql_manager import SQL

client = SQL("grades")
client.create_table('./dataset/student_course_grades.csv')
client.create_log_table()
row = client.get({
    'student_id': 'SID1033',
    'course_id': 'CSE016'
})

print(row)