set hive.cli.print.header=true;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;
set hive.stats.column.autogather=false;

-- SORT_QUERY_RESULTS

create table student_10_lines_txt(
name string,
age int,
gpa double)
row format delimited
fields terminated by '\001'
stored as textfile;
LOAD DATA LOCAL INPATH '../../data/files/student_10_lines' OVERWRITE INTO TABLE student_10_lines_txt;
CREATE TABLE student_10_lines STORED AS ORC AS SELECT * FROM student_10_lines_txt;
INSERT INTO TABLE student_10_lines VALUES (NULL, NULL, NULL);
INSERT INTO TABLE student_10_lines VALUES ("George", 22, 3.8);
analyze table student_10_lines compute statistics;

------------------------------------------------------------------------------------------

SET hive.vectorized.if.expr.mode=adaptor;

create table insert_a_adaptor (name string, age int, gpa double, a int, b timestamp, c string, d binary, e int, f double);

explain vectorization detail
insert overwrite table insert_a_adaptor
    select
      name,
      age,
      gpa,
      IF(age<40, age, NULL),
      IF(age>40, cast('2011-01-01 01:01:01' as timestamp), NULL),
      IF(LENGTH(name)>8, name, NULL),
      IF(LENGTH(name)<8, cast(name as binary), NULL),
      IF(age>40, LENGTH(name), NULL),
      IF(LENGTH(name)> 10, 2 * gpa, NULL)
    from student_10_lines;
insert overwrite table insert_a_adaptor
    select
      name,
      age,
      gpa,
      IF(age<40, age, NULL),
      IF(age>40, cast('2011-01-01 01:01:01' as timestamp), NULL),
      IF(LENGTH(name)>8, name, NULL),
      IF(LENGTH(name)<8, cast(name as binary), NULL),
      IF(age>40, LENGTH(name), NULL),
      IF(LENGTH(name)> 10, 2 * gpa, NULL)
    from student_10_lines;
select * from insert_a_adaptor;

SET hive.vectorized.if.expr.mode=good;

create table insert_a_good (name string, age int, gpa double, a int, b timestamp, c string, d binary, e int, f double);

explain vectorization detail
insert overwrite table insert_a_good
    select
      name,
      age,
      gpa,
      IF(age<40, age, NULL),
      IF(age>40, cast('2011-01-01 01:01:01' as timestamp), NULL),
      IF(LENGTH(name)>8, name, NULL),
      IF(LENGTH(name)<8, cast(name as binary), NULL),
      IF(age>40, LENGTH(name), NULL),
      IF(LENGTH(name)> 10, 2 * gpa, NULL)
    from student_10_lines;
insert overwrite table insert_a_good
    select
      name,
      age,
      gpa,
      IF(age<40, age, NULL),
      IF(age>40, cast('2011-01-01 01:01:01' as timestamp), NULL),
      IF(LENGTH(name)>8, name, NULL),
      IF(LENGTH(name)<8, cast(name as binary), NULL),
      IF(age>40, LENGTH(name), NULL),
      IF(LENGTH(name)> 10, 2 * gpa, NULL)
    from student_10_lines;
select * from insert_a_good;

SET hive.vectorized.if.expr.mode=better;

create table insert_a_better (name string, age int, gpa double, a int, b timestamp, c string, d binary, e int, f double);

explain vectorization detail
insert overwrite table insert_a_better
    select
      name,
      age,
      gpa,
      IF(age<40, age, NULL),
      IF(age>40, cast('2011-01-01 01:01:01' as timestamp), NULL),
      IF(LENGTH(name)>8, name, NULL),
      IF(LENGTH(name)<8, cast(name as binary), NULL),
      IF(age>40, LENGTH(name), NULL),
      IF(LENGTH(name)> 10, 2 * gpa, NULL)
    from student_10_lines;
insert overwrite table insert_a_better
    select
      name,
      age,
      gpa,
      IF(age<40, age, NULL),
      IF(age>40, cast('2011-01-01 01:01:01' as timestamp), NULL),
      IF(LENGTH(name)>8, name, NULL),
      IF(LENGTH(name)<8, cast(name as binary), NULL),
      IF(age>40, LENGTH(name), NULL),
      IF(LENGTH(name)> 10, 2 * gpa, NULL)
    from student_10_lines;
select * from insert_a_better;

------------------------------------------------------------------------------------------

SET hive.vectorized.if.expr.mode=adaptor;

create table insert_b_adaptor (name string, age int, gpa double, a int, b timestamp, c string, d binary, e int, f double);

explain vectorization detail
insert overwrite table insert_b_adaptor
    select
      name,
      age,
      gpa,
      IF(age<40, NULL, age),
      IF(age>40, NULL, cast('2011-01-01 01:01:01' as timestamp)),
      IF(LENGTH(name)>8, NULL, name),
      IF(LENGTH(name)<8, NULL, cast(name as binary)),
      IF(age>40, NULL, LENGTH(name)),
      IF(LENGTH(name)> 10, NULL, 2 * gpa)
    from student_10_lines;
insert overwrite table insert_b_adaptor
    select
      name,
      age,
      gpa,
      IF(age<40, NULL, age),
      IF(age>40, NULL, cast('2011-01-01 01:01:01' as timestamp)),
      IF(LENGTH(name)>8, NULL, name),
      IF(LENGTH(name)<8, NULL, cast(name as binary)),
      IF(age>40, NULL, LENGTH(name)),
      IF(LENGTH(name)> 10, NULL, 2 * gpa)
    from student_10_lines;
select * from insert_b_adaptor;

SET hive.vectorized.if.expr.mode=good;

create table insert_b_good (name string, age int, gpa double, a int, b timestamp, c string, d binary, e int, f double);

explain vectorization detail
insert overwrite table insert_b_good
    select
      name,
      age,
      gpa,
      IF(age<40, NULL, age),
      IF(age>40, NULL, cast('2011-01-01 01:01:01' as timestamp)),
      IF(LENGTH(name)>8, NULL, name),
      IF(LENGTH(name)<8, NULL, cast(name as binary)),
      IF(age>40, NULL, LENGTH(name)),
      IF(LENGTH(name)> 10, NULL, 2 * gpa)
    from student_10_lines;
insert overwrite table insert_b_good
    select
      name,
      age,
      gpa,
      IF(age<40, NULL, age),
      IF(age>40, NULL, cast('2011-01-01 01:01:01' as timestamp)),
      IF(LENGTH(name)>8, NULL, name),
      IF(LENGTH(name)<8, NULL, cast(name as binary)),
      IF(age>40, NULL, LENGTH(name)),
      IF(LENGTH(name)> 10, NULL, 2 * gpa)
    from student_10_lines;
select * from insert_b_good;

SET hive.vectorized.if.expr.mode=better;

create table insert_b_better (name string, age int, gpa double, a int, b timestamp, c string, d binary, e int, f double);

explain vectorization detail
insert overwrite table insert_b_better
    select
      name,
      age,
      gpa,
      IF(age<40, NULL, age),
      IF(age>40, NULL, cast('2011-01-01 01:01:01' as timestamp)),
      IF(LENGTH(name)>8, NULL, name),
      IF(LENGTH(name)<8, NULL, cast(name as binary)),
      IF(age>40, NULL, LENGTH(name)),
      IF(LENGTH(name)> 10, NULL, 2 * gpa)
    from student_10_lines;
insert overwrite table insert_b_better
    select
      name,
      age,
      gpa,
      IF(age<40, NULL, age),
      IF(age>40, NULL, cast('2011-01-01 01:01:01' as timestamp)),
      IF(LENGTH(name)>8, NULL, name),
      IF(LENGTH(name)<8, NULL, cast(name as binary)),
      IF(age>40, NULL, LENGTH(name)),
      IF(LENGTH(name)> 10, NULL, 2 * gpa)
    from student_10_lines;
select * from insert_b_better;