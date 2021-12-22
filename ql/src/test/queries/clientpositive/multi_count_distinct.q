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


create table test_count (c0 string, c1 string, c2 string, c3 string, c4 string, c5 string, c6 string, c7 string,
                         c8 string, c9 string, c10 string, c11 string, c12 string, c13 string, c14 string, c15 string,
                         c16 string, c17 string, c18 string, c19 string, c20 string, c21 string, c22 string, c23 string,
                         c24 string, c25 string, c26 string, c27 string, c28 string, c29 string, c30 string, c31 string,
                         c32 string, c33 string, c34 string, c35 string, c36 string, c37 string, c38 string, c39 string,
                         c40 string, c41 string, c42 string, c43 string, c44 string, c45 string, c46 string, c47 string,
                         c48 string, c49 string, c50 string, c51 string, c52 string, c53 string, c54 string, c55 string,
                         c56 string, c57 string, c58 string, c59 string, c60 string, c61 string, c62 string, c63 string,
                         c64 string);
INSERT INTO test_count values ('c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8', 'c9', 'c10', 'c11', 'c12', 'c13', 'c14', 'c15',
              'c16', 'c17', 'c18', 'c19', 'c20', 'c21', 'c22', 'c23', 'c24', 'c25', 'c26', 'c27', 'c28', 'c29', 'c30', 'c31', 'c32',
              'c33', 'c34', 'c35', 'c36', 'c37', 'c38', 'c39', 'c40', 'c41', 'c42', 'c43', 'c44', 'c45', 'c46', 'c47', 'c48', 'c49',
              'c50', 'c51', 'c52', 'c53', 'c54', 'c55', 'c56', 'c57', 'c58', 'c59', 'c60', 'c61', 'c62', 'c63', 'c64');

-- query is rewritten when there are fewer than 64 count(distinct) expressions
explain extended select count (distinct c0), count(distinct c1), count(distinct c2), count(distinct c3), count(distinct c4), count(distinct c5),
       count(distinct c6), count(distinct c7), count(distinct c8), count(distinct c9), count(distinct c10), count(distinct c11),
       count(distinct c12), count(distinct c13), count(distinct c14), count(distinct c15), count(distinct c16), count(distinct c17),
       count(distinct c18), count(distinct c19), count(distinct c20), count(distinct c21), count(distinct c22), count(distinct c23),
       count(distinct c24), count(distinct c25), count(distinct c26), count(distinct c27), count(distinct c28), count(distinct c29),
       count(distinct c30), count(distinct c31), count(distinct c32) from test_count;

select count (distinct c0), count(distinct c1), count(distinct c2), count(distinct c3), count(distinct c4), count(distinct c5),
       count(distinct c6), count(distinct c7), count(distinct c8), count(distinct c9), count(distinct c10), count(distinct c11),
       count(distinct c12), count(distinct c13), count(distinct c14), count(distinct c15), count(distinct c16), count(distinct c17),
       count(distinct c18), count(distinct c19), count(distinct c20), count(distinct c21), count(distinct c22), count(distinct c23),
       count(distinct c24), count(distinct c25), count(distinct c26), count(distinct c27), count(distinct c28), count(distinct c29),
       count(distinct c30), count(distinct c31), count(distinct c32) from test_count;

-- query is not rewritten when there are more than 63 count(distinct) expressions
explain extended select count (distinct c0), count(distinct c1), count(distinct c2), count(distinct c3), count(distinct c4), count(distinct c5),
       count(distinct c6), count(distinct c7), count(distinct c8), count(distinct c9), count(distinct c10), count(distinct c11),
       count(distinct c12), count(distinct c13), count(distinct c14), count(distinct c15), count(distinct c16), count(distinct c17),
       count(distinct c18), count(distinct c19), count(distinct c20), count(distinct c21), count(distinct c22), count(distinct c23),
       count(distinct c24), count(distinct c25), count(distinct c26), count(distinct c27), count(distinct c28), count(distinct c29),
       count(distinct c30), count(distinct c31), count(distinct c32), count(distinct c33), count(distinct c34), count(distinct c35),
       count(distinct c36), count(distinct c37), count(distinct c38), count(distinct c39), count(distinct c40), count(distinct c41),
       count(distinct c42), count(distinct c43), count(distinct c44), count(distinct c45), count(distinct c46), count(distinct c47),
       count(distinct c48), count(distinct c49), count(distinct c50), count(distinct c51), count(distinct c52), count(distinct c53),
       count(distinct c54), count(distinct c55), count(distinct c56), count(distinct c57), count(distinct c58), count(distinct c59),
       count(distinct c62), count(distinct c61), count(distinct c62), count(distinct c63), count(distinct c64) from test_count;

select count (distinct c0), count(distinct c1), count(distinct c2), count(distinct c3), count(distinct c4), count(distinct c5),
       count(distinct c6), count(distinct c7), count(distinct c8), count(distinct c9), count(distinct c10), count(distinct c11),
       count(distinct c12), count(distinct c13), count(distinct c14), count(distinct c15), count(distinct c16), count(distinct c17),
       count(distinct c18), count(distinct c19), count(distinct c20), count(distinct c21), count(distinct c22), count(distinct c23),
       count(distinct c24), count(distinct c25), count(distinct c26), count(distinct c27), count(distinct c28), count(distinct c29),
       count(distinct c30), count(distinct c31), count(distinct c32), count(distinct c33), count(distinct c34), count(distinct c35),
       count(distinct c36), count(distinct c37), count(distinct c38), count(distinct c39), count(distinct c40), count(distinct c41),
       count(distinct c42), count(distinct c43), count(distinct c44), count(distinct c45), count(distinct c46), count(distinct c47),
       count(distinct c48), count(distinct c49), count(distinct c50), count(distinct c51), count(distinct c52), count(distinct c53),
       count(distinct c54), count(distinct c55), count(distinct c56), count(distinct c57), count(distinct c58), count(distinct c59),
       count(distinct c62), count(distinct c61), count(distinct c62), count(distinct c63), count(distinct c64) from test_count;

select count (distinct c0), count(distinct c1), count(distinct c2), count(distinct c3), count(distinct c4), count(distinct c5),
       count(distinct c6), count(distinct c7), count(distinct c8), count(distinct c9), count(distinct c10), count(distinct c11),
       count(distinct c12), count(distinct c13), count(distinct c14), count(distinct c15), count(distinct c16), count(distinct c17),
       count(distinct c18), count(distinct c19), count(distinct c20), count(distinct c21), count(distinct c22), count(distinct c23),
       count(distinct c24), count(distinct c25), count(distinct c26), count(distinct c27), count(distinct c28), count(distinct c29),
       count(distinct c30), count(distinct c31), count(distinct c32), count(distinct c33), count(distinct c34), count(distinct c35),
       count(distinct c36), count(distinct c37), count(distinct c38), count(distinct c39), count(distinct c40), count(distinct c41),
       count(distinct c42), count(distinct c43), count(distinct c44), count(distinct c45), count(distinct c46), count(distinct c47),
       count(distinct c48), count(distinct c49), count(distinct c50), count(distinct c51), count(distinct c52), count(distinct c53),
       count(distinct c54), count(distinct c55), count(distinct c56), count(distinct c57), count(distinct c58), count(distinct c59),
       count(distinct c62), count(distinct c61) from test_count
group by c64,c0||c1,c0||c2,c0||c3,c0||c4,c0||c5,c0||c6;
