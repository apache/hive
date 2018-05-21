--! qt:dataset:src
-- SORT_QUERY_RESULTS

drop table t1_n28;
drop table t2_n19;


create table t1_n28 as select * from src where key < 10;
create table t2_n19 as select * from src where key < 10;

create table t3_n6(key string, cnt int);
create table t4_n2(value string, cnt int);

explain
from
(select * from t1_n28
 union all
 select * from t2_n19
) x
insert overwrite table t3_n6
  select key, count(1) group by key
insert overwrite table t4_n2
  select value, count(1) group by value;

from
(select * from t1_n28
 union all
 select * from t2_n19
) x
insert overwrite table t3_n6
  select key, count(1) group by key
insert overwrite table t4_n2
  select value, count(1) group by value;

select * from t3_n6;
select * from t4_n2;

create table t5_n0(c1 string, cnt int);
create table t6_n0(c1 string, cnt int);

explain
from
(
 select key as c1, count(1) as cnt from t1_n28 group by key
   union all
 select key as c1, count(1) as cnt from t2_n19 group by key
) x
insert overwrite table t5_n0
  select c1, sum(cnt) group by c1
insert overwrite table t6_n0
  select c1, sum(cnt) group by c1;

from
(
 select key as c1, count(1) as cnt from t1_n28 group by key
   union all
 select key as c1, count(1) as cnt from t2_n19 group by key
) x
insert overwrite table t5_n0
  select c1, sum(cnt) group by c1
insert overwrite table t6_n0
  select c1, sum(cnt) group by c1;

select * from t5_n0;
select * from t6_n0;

drop table t1_n28;
drop table t2_n19;

create table t1_n28 as select * from src where key < 10;
create table t2_n19 as select key, count(1) as cnt from src where key < 10 group by key;

create table t7_n1(c1 string, cnt int);
create table t8_n0(c1 string, cnt int);

explain
from
(
 select key as c1, count(1) as cnt from t1_n28 group by key
   union all
 select key as c1, cnt from t2_n19
) x
insert overwrite table t7_n1
  select c1, count(1) group by c1
insert overwrite table t8_n0
  select c1, count(1) group by c1;

from
(
 select key as c1, count(1) as cnt from t1_n28 group by key
   union all
 select key as c1, cnt from t2_n19
) x
insert overwrite table t7_n1
  select c1, count(1) group by c1
insert overwrite table t8_n0
  select c1, count(1) group by c1;

select * from t7_n1;
select * from t8_n0;
