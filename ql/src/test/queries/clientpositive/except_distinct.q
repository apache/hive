--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;

create table a_n16(key int);

insert into table a_n16 values (0),(1),(2),(2),(2),(2),(3),(NULL),(NULL);

create table b_n12(key bigint);

insert into table b_n12 values (1),(2),(2),(3),(5),(5),(NULL),(NULL),(NULL);

select * from a_n16 except distinct select * from b_n12;

drop table a_n16;

drop table b_n12;

create table a_n16(key int, value int);

insert into table a_n16 values (1,2),(1,2),(1,3),(2,3),(2,2);

create table b_n12(key int, value int);

insert into table b_n12 values (1,2),(2,3),(2,2),(2,2),(2,20);

select * from a_n16 except distinct select * from b_n12;

select * from b_n12 except distinct select * from a_n16;

select * from b_n12 except distinct select * from a_n16 intersect distinct select * from b_n12;

select * from b_n12 except distinct select * from a_n16 except distinct select * from b_n12;

select * from a_n16 except distinct select * from b_n12 union all select * from a_n16 except distinct select * from b_n12;

select * from a_n16 except distinct select * from b_n12 union select * from a_n16 except distinct select * from b_n12;

select * from a_n16 except distinct select * from b_n12 except distinct select * from a_n16 except distinct select * from b_n12;

select * from (select a_n16.key, b_n12.value from a_n16 join b_n12 on a_n16.key=b_n12.key)sub1 
except distinct 
select * from (select a_n16.key, b_n12.value from a_n16 join b_n12 on a_n16.key=b_n12.key)sub2; 

select * from (select a_n16.key, b_n12.value from a_n16 join b_n12 on a_n16.key=b_n12.key)sub1
except distinct
select * from (select b_n12.value as key, a_n16.key as value from a_n16 join b_n12 on a_n16.key=b_n12.key)sub2;

explain select * from src except distinct select * from src;

select * from src except distinct select * from src;

explain select * from src except distinct select * from src except distinct select * from src except distinct select * from src;

select * from src except distinct select * from src except distinct select * from src except distinct select * from src;

explain select value from a_n16 group by value except distinct select key from b_n12 group by key;

select value from a_n16 group by value except distinct select key from b_n12 group by key;
