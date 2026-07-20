CREATE TABLE studenttab10k(
 name string,
 age int,
 gpa double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/studenttab10k' OVERWRITE INTO TABLE studenttab10k;


set hive.support.quoted.identifiers=column;

drop table if exists quoted_identifier_2_1;

create table quoted_identifier_2_1(`-+={[<_name_>]}?/|` string, `!@#$%^&*()_age` int) partitioned by(`gpa_!@#$%^&*()` double) stored by iceberg;

insert into table quoted_identifier_2_1 select name,age,gpa from studenttab10k where gpa=3.5;

drop view if exists quoted_identifier_2_2;

create view quoted_identifier_2_2 as select `!@#$%^&*()_age`,count(*)  from quoted_identifier_2_1 group by `!@#$%^&*()_age`;

-- Two partition columns: one plain (age), one with special characters (gpa_!@#$%^&*())
drop table if exists quoted_identifier_2_3;

create table quoted_identifier_2_3(`name_col` string) partitioned by(age int, `gpa_!@#$%^&*()` double) stored by iceberg;

insert into table quoted_identifier_2_3 select name,age,gpa from studenttab10k where gpa=4.5;

select count(*) from quoted_identifier_2_3;
