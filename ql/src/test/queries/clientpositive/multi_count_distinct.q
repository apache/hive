SET hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;

drop table employee_n1;

create table employee_n1 (department_id int, gender varchar(10), education_level int);
 
insert into employee_n1 values (1, 'M', 1),(1, 'M', 1),(2, 'F', 1),(1, 'F', 3),(1, 'M', 2),(4, 'M', 1),(2, 'F', 1),(2, 'F', 3),(3, 'M', 2);

explain select count(distinct department_id), count(distinct gender), count(distinct education_level) from employee_n1;

select count(distinct department_id), count(distinct gender), count(distinct education_level) from employee_n1;

select count(distinct department_id), count(distinct gender), count(distinct education_level), count(distinct education_level) from employee_n1;

select count(distinct department_id), count(distinct gender), count(distinct education_level), 
count(distinct education_level, department_id) from employee_n1;

select count(distinct gender), count(distinct department_id), count(distinct gender), count(distinct education_level),
count(distinct education_level, department_id), count(distinct department_id, education_level) from employee_n1;

explain select count(distinct gender), count(distinct department_id), count(distinct gender), count(distinct education_level),
count(distinct education_level, department_id), count(distinct department_id, education_level), count(distinct department_id, education_level, gender) from employee_n1;

select count(distinct gender), count(distinct department_id), count(distinct gender), count(distinct education_level),
count(distinct education_level, department_id), count(distinct department_id, education_level), count(distinct department_id, education_level, gender) from employee_n1;

select count(case i when 3 then 1 else null end) as c0, count(case i when 5 then 1 else null end) as c1, 
count(case i when 6 then 1 else null end) as c2 from (select grouping__id as i, department_id, gender, 
education_level from employee_n1 group by department_id, gender, education_level grouping sets 
(department_id, gender, education_level))subq;

select grouping__id as i, department_id, gender, education_level from employee_n1 
group by department_id, gender, education_level grouping sets 
(department_id, gender, education_level, (education_level, department_id));




