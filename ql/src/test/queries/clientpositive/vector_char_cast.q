set hive.fetch.task.conversion=none;

create table s1_n2(id smallint) stored as orc;

insert into table s1_n2 values (1000),(1001),(1002),(1003),(1000);

set hive.vectorized.execution.enabled=true;
select count(1) from s1_n2 where cast(id as char(4))='1000';

set hive.vectorized.execution.enabled=false;
select count(1) from s1_n2 where cast(id as char(4))='1000';