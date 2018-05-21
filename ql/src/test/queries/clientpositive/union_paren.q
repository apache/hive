--! qt:dataset:src
set hive.mapred.mode=nonstrict;

explain select * from src union all select * from src;

create table t1_n3(c int);

insert into t1_n3 values (1),(1),(2);

create table t2_n2(c int);

insert into t2_n2 values (2),(1),(2);

create table t3_n1(c int);

insert into t3_n1 values (2),(3),(2);

(select * from t1_n3) union all select * from t2_n2 union select * from t3_n1 order by c;

(select * from t1_n3) union all (select * from t2_n2 union select * from t3_n1) order by c;

(select * from src order by key limit 1);

(select * from src) union all select * from src order by key limit 1;

(select * from src limit 1) union all select * from src order by key limit 1;

((select * from src)) union all select * from src order by key limit 1;

select * from src union all ((select * from src)) order by key limit 1;

select * from src union all ((select * from src limit 1)) order by key limit 1;

select * from src union all (select * from src) order by key limit 1;

(select * from src order by key) union all (select * from src) order by key limit 1;

(select * from src order by key) union all (select * from src limit 1) order by key limit 1;

select count(*) from (select key from src union select key from src)cool_cust;

--similar tpcds q14

with  cross_items as
 (select key, k
 from src,
 (select iss.key k
 from src iss
 union all
 select ics.key k
 from src ics
 ) x
 where key = k
)
select * from cross_items order by key limit 1;
