--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;

create table a_n15(key int);

insert into table a_n15 values (0),(1),(2),(2),(2),(2),(3),(NULL),(NULL);

create table b_n11(key bigint);

insert into table b_n11 values (1),(2),(2),(3),(5),(5),(NULL),(NULL),(NULL);

select * from a_n15 except all select * from b_n11;

drop table a_n15;

drop table b_n11;

create table a_n15(key int, value int);

insert into table a_n15 values (1,2),(1,2),(1,3),(2,3),(2,2);

create table b_n11(key int, value int);

insert into table b_n11 values (1,2),(2,3),(2,2),(2,2),(2,20);

select * from a_n15 except all select * from b_n11;

select * from b_n11 except all select * from a_n15;

select * from b_n11 except all select * from a_n15 intersect distinct select * from b_n11;

select * from b_n11 except all select * from a_n15 except distinct select * from b_n11;

select * from a_n15 except all select * from b_n11 union all select * from a_n15 except distinct select * from b_n11;

select * from a_n15 except all select * from b_n11 union select * from a_n15 except distinct select * from b_n11;

select * from a_n15 except all select * from b_n11 except distinct select * from a_n15 except distinct select * from b_n11;

select * from (select a_n15.key, b_n11.value from a_n15 join b_n11 on a_n15.key=b_n11.key)sub1 
except all 
select * from (select a_n15.key, b_n11.value from a_n15 join b_n11 on a_n15.key=b_n11.key)sub2; 

select * from (select a_n15.key, b_n11.value from a_n15 join b_n11 on a_n15.key=b_n11.key)sub1
except all
select * from (select b_n11.value as key, a_n15.key as value from a_n15 join b_n11 on a_n15.key=b_n11.key)sub2;

explain select * from src except all select * from src;

select * from src except all select * from src;

explain select * from src except all select * from src except distinct select * from src except distinct select * from src;

select * from src except all select * from src except distinct select * from src except distinct select * from src;

explain select value from a_n15 group by value except distinct select key from b_n11 group by key;

select value from a_n15 group by value except distinct select key from b_n11 group by key;
