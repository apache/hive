--! qt:dataset:src
set hive.cbo.enable=false;
set hive.mapred.mode=nonstrict;


explain
select 1 as a from src
union all
select 1 as a from src limit 1;

explain
select a, key, value from
(
select 1 as a from src
union all
select 1 as a from src limit 1
)sub join src b where value='12345';


explain
select 1 as a from src
union all
select 2 as a from src limit 1;

explain
select a, key, value from
(
select 1 as a from src
union all
select 2 as a from src limit 1
)sub join src b where value='12345';

explain
select a.key, b.value from src a join src b where a.key = '238' and b.value = '234';

explain
select a.key, b.value from src a join src b on a.key=b.key where b.value = '234';

create table t_n26 (
a int,
b int,
c int,
d int,
e int
);

explain 
select a2 as a3 from
(select a1 as a2, c1 as c2 from
(select a as a1, b as b1, c as c1 from t_n26 where a=1 and b=2 and c=3)sub1)sub2; 



