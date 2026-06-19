-- SORT_QUERY_RESULTS

set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;

create table a_n1(key int, value int);

insert into table a_n1 values (1,2),(1,2),(1,2),(1,3),(2,3);

create table b_n1(key int, value int);

insert into table b_n1 values (1,2),(1,2),(2,3);

select * from a_n1 intersect select * from b_n1;

(select * from b_n1 intersect (select * from a_n1)) intersect select * from b_n1;

select * from b_n1 intersect all select * from a_n1 intersect select * from b_n1;

(select * from b_n1) intersect all ((select * from a_n1) intersect select * from b_n1);

select * from (select a_n1.key, b_n1.value from a_n1 join b_n1 on a_n1.key=b_n1.key)sub1 
intersect 
select * from (select a_n1.key, b_n1.value from a_n1 join b_n1 on a_n1.key=b_n1.key)sub2; 

drop table a_n1;

drop table b_n1;

create table a_n1(key int);

insert into table a_n1 values (0),(1),(2),(2),(2),(2),(3),(NULL),(NULL),(NULL),(NULL),(NULL);

create table b_n1(key bigint);

insert into table b_n1 values (1),(2),(2),(3),(5),(5),(NULL),(NULL),(NULL);

select * from a_n1 except select * from b_n1;

(select * from a_n1) minus select * from b_n1 union (select * from a_n1) minus select * from b_n1;

(select * from a_n1) minus select * from b_n1 union all ((select * from a_n1) minus select * from b_n1);

(select * from a_n1) minus select * from b_n1 union all (select * from a_n1) minus all select * from b_n1;

select * from a_n1 minus select * from b_n1 minus (select * from a_n1 minus select * from b_n1);

(select * from a_n1) minus (select * from b_n1 minus (select * from a_n1 minus select * from b_n1));

drop table a_n1;

drop table b_n1;

