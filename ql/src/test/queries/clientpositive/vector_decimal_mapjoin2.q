set hive.vectorized.execution.enabled=true;
set hive.auto.convert.join=true;

DROP TABLE IF EXISTS salary;
CREATE EXTERNAL TABLE salary(pay_date TIMESTAMP,employee_id INT,department_id INT,currency_id INT,salary_paid DECIMAL(10,4),overtime_paid DECIMAL(10,4),vacation_accrued DOUBLE,vacation_used DOUBLE)
row format delimited fields terminated by '\011'
location '../../data/files/salary';

DROP TABLE IF EXISTS employee_closure;
CREATE EXTERNAL TABLE employee_closure(employee_id INT,supervisor_id INT,distance INT)
row format delimited fields terminated by '\011'
location '../../data/files/employee_closure';

select sum(salary.salary_paid) from salary, employee_closure where salary.employee_id = employee_closure.employee_id;