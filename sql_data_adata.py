import mysql.connector
from mysql.connector import Error
import csv

pw = 'fromhell' #passwor for database

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def execute_query(connection, query):
    """For query to DB"""
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


create_departments_table = """
CREATE TABLE departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(50) NOT NULL
);"""

jods_table = """
CREATE TABLE job_titles (
    job_title_id INT PRIMARY KEY,
    title_name VARCHAR(50) NOT NULL
);"""


create_employees_table = """
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    full_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    hire_date DATE NOT NULL,
    department_id INT,
    job_title_id INT,
    FOREIGN KEY (department_id) REFERENCES departments (department_id),
    FOREIGN KEY (job_title_id) REFERENCES job_titles (job_title_id)
);"""

create_table_salary = """
CREATE TABLE salaries (
    salary_id INT PRIMARY KEY,
    employee_id INT,
    salary DECIMAL(10, 2),
    start_date DATE NOT NULL,
    end_date DATE,
    FOREIGN KEY (employee_id) REFERENCES employees (employee_id)
);"""

table__5 = """CREATE TABLE employee_job_history (
    history_id INT PRIMARY KEY,
    employee_id INT,
    job_title_id INT,
    department_id INT,
    start_date DATE NOT NULL,
    end_date DATE,
    FOREIGN KEY (employee_id) REFERENCES employees (employee_id),
    FOREIGN KEY (job_title_id) REFERENCES job_titles (job_title_id),
    FOREIGN KEY (department_id) REFERENCES departments (department_id)
);"""

connection = create_db_connection("localhost", "ME", pw, "mydatabase") # Connect to the Database
#execute_query(connection, jods_table)
#execute_query(connection, create_employees_table)
#execute_query(connection, table__5)
#execute_query(connection, create_table_salary)


add_data_depa = """
INSERT INTO departments (department_id, department_name)
VALUES
    (1, 'Design'),
    (2, 'Finance'),
    (3, 'IT'),
    (4, 'Marketing');"""

add_values_to_jobs = """
INSERT INTO job_titles (job_title_id, title_name)
VALUES
    (1, 'Manager'),
    (2, 'Developer'),
    (3, 'Accountant'),
    (4, 'Marketing Specialist');"""

add_values_to_table_employees = """
INSERT INTO employees (employee_id, full_name, email, hire_date, department_id, job_title_id)
VALUES
    (1, 'John Doe', 'john.doe@example.com', '2020-01-15', 1, 1),
    (2, 'Jane Smith', 'jane.smith@example.com', '2019-05-20', 2, 2),
    (3, 'Michael Johnson', 'michael.johnson@example.com', '2021-03-10', 3, 2),
    (4, 'Emily Brown', 'emily.brown@example.com', '2018-11-05', 1, 3),
    (5, 'David Lee', 'david.lee@example.com', '2022-02-28', 2, 4);"""

add_values_salary = """
INSERT INTO salaries (salary_id, employee_id, salary, start_date, end_date)
VALUES
    (1, 1, 65000.00, '2020-01-15', '2021-12-31'),
    (2, 1, 70000.00, '2022-01-01', NULL),
    (3, 2, 75000.00, '2019-05-20', '2022-05-31'),
    (4, 3, 80000.00, '2021-03-10', NULL),
    (5, 4, 55000.00, '2018-11-05', NULL);"""

add_values_to_table_5 = """INSERT INTO employee_job_history (history_id, employee_id, job_title_id, department_id, start_date, end_date)
VALUES
    (1, 1, 1, 1, '2020-01-15', '2022-05-31'),
    (2, 1, 2, 2, '2022-06-01', NULL),
    (3, 2, 2, 2, '2019-05-20', NULL),
    (4, 3, 2, 3, '2021-03-10', NULL),
    (5, 4, 3, 1, '2018-11-05', '2020-12-31');"""

#execute_query(connection, add_data_depa)
#execute_query(connection, add_values_to_jobs)
#execute_query(connection, add_values_to_table_employees)
#execute_query(connection, add_values_salary)
#execute_query(connection, add_values_to_table_5)

task_1 = """SELECT
    e.full_name,
    s.salary AS salary,
    jt.title_name AS position
FROM
    employees e
JOIN
    departments d ON e.department_id = d.department_id
JOIN
    job_titles jt ON e.job_title_id = jt.job_title_id
LEFT JOIN
    salaries s ON e.employee_id = s.employee_id
WHERE
    e.full_name LIKE '%Jane%' AND d.department_name = 'Finance';"""



task_2 = """
SELECT
    d.department_name,
    AVG(s.salary) AS average_salary
FROM
    employees e
JOIN
    departments d ON e.department_id = d.department_id
JOIN
    salaries s ON e.employee_id = s.employee_id
GROUP BY
    d.department_name;"""


task_3 = """SELECT
    jt.title_name AS position,
    AVG(s.salary) AS average_salary,
    CASE
        WHEN AVG(s.salary) > (SELECT AVG(salary) FROM salaries) THEN 'Yes'
        ELSE 'No'
    END AS above_average
FROM
    employees e
JOIN
    job_titles jt ON e.job_title_id = jt.job_title_id
JOIN
    salaries s ON e.employee_id = s.employee_id
GROUP BY
    jt.title_name;"""

def get_query(Query, file_name):
    cursor = connection.cursor()
    cursor.execute(Query)
    with open(f"{file_name}.csv", mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([x[0] for x in cursor.description])
        writer.writerows(cursor.fetchall())


get_query("""SELECT * FROM departments""", 'table_departments')
get_query("""SELECT * FROM employees""", 'table_employees')
get_query("""SELECT * FROM job_titles""", 'table_job_titles')
get_query("""SELECT * FROM salaries""", 'table_salaries')
get_query("""SELECT * FROM employee_job_history""", 'table_employee_job_history')

#get_query(task_1, 'task_1')
#get_query(task_2, 'task_2')
#get_query(task_3, 'task_3')
