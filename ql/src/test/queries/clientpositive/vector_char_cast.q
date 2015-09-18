create table s1(id smallint) stored as orc;

insert into table s1 values (1000),(1001),(1002),(1003),(1000);

set hive.vectorized.execution.enabled=true;
select count(1) from s1 where cast(id as char(4))='1000';

set hive.vectorized.execution.enabled=false;
select count(1) from s1 where cast(id as char(4))='1000';