set hive.support.quoted.identifiers=column;
set orc.force.positional.evolution=true;
set hive.vectorized.execution.enabled=false;

CREATE TABLE studenttab10k(
 name string,
 age int,
 gpa double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/studenttab10k' OVERWRITE INTO TABLE studenttab10k;

create table temptable1(
 `!@#$%^&*()_name` string,
  age int,
  gpa double
) stored as orc;

insert overwrite table temptable1 select * from studenttab10k;

alter table temptable1 change age `!@#$%^&*()_age` int;
alter table temptable1 change gpa `!@#$%^&*()_gpa` double;

select `!@#$%^&*()_age`, count(*) from temptable1 group by `!@#$%^&*()_age` order by `!@#$%^&*()_age`;

drop table temptable1;
set hive.vectorized.execution.enabled=true;

create table temptable1(
 `!@#$%^&*()_name` string,
  age int,
  gpa double
) stored as orc;

insert overwrite table temptable1 select * from studenttab10k;

alter table temptable1 change age `!@#$%^&*()_age` int;
alter table temptable1 change gpa `!@#$%^&*()_gpa` double;

select `!@#$%^&*()_age`, count(*) from temptable1 group by `!@#$%^&*()_age` order by `!@#$%^&*()_age`;
