SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;
set hive.stats.column.autogather=false;

create table student_2_lines(
name string,
age int,
gpa double)
row format delimited
fields terminated by '\001'
stored as textfile;
LOAD DATA LOCAL INPATH '../../data/files/student_2_lines' OVERWRITE INTO TABLE student_2_lines;
analyze table student_2_lines compute statistics;

create table insert_10_1 (a float, b int, c timestamp, d binary);

explain vectorization detail
insert overwrite table insert_10_1
    select cast(gpa as float),
    age,
    IF(age>40,cast('2011-01-01 01:01:01' as timestamp),NULL),
    IF(LENGTH(name)>10,cast(name as binary),NULL) from student_2_lines;
insert overwrite table insert_10_1
    select cast(gpa as float),
    age,
    IF(age>40,cast('2011-01-01 01:01:01' as timestamp),NULL),
    IF(LENGTH(name)>10,cast(name as binary),NULL) from student_2_lines;