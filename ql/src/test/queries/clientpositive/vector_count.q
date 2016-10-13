set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

create table abcd_txt (a int, b int, c int, d int);
LOAD DATA LOCAL INPATH '../../data/files/in4.txt' INTO TABLE abcd_txt;

create table abcd stored as orc as select * from abcd_txt;

select * from abcd;
set hive.map.aggr=true;
explain vectorization expression select a, count(distinct b), count(distinct c), sum(d) from abcd group by a;
select a, count(distinct b), count(distinct c), sum(d) from abcd group by a;

explain vectorization expression select count(1), count(*), count(a), count(b), count(c), count(d), count(distinct a), count(distinct b), count(distinct c), count(distinct d), count(distinct a,b), count(distinct b,c), count(distinct c,d), count(distinct a,d), count(distinct a,c), count(distinct b,d), count(distinct a,b,c), count(distinct b,c,d), count(distinct a,c,d), count(distinct a,b,d), count(distinct a,b,c,d) from abcd;
select count(1), count(*), count(a), count(b), count(c), count(d), count(distinct a), count(distinct b), count(distinct c), count(distinct d), count(distinct a,b), count(distinct b,c), count(distinct c,d), count(distinct a,d), count(distinct a,c), count(distinct b,d), count(distinct a,b,c), count(distinct b,c,d), count(distinct a,c,d), count(distinct a,b,d), count(distinct a,b,c,d) from abcd;

set hive.map.aggr=false;
explain vectorization expression select a, count(distinct b), count(distinct c), sum(d) from abcd group by a;
select a, count(distinct b), count(distinct c), sum(d) from abcd group by a;

explain vectorization expression select count(1), count(*), count(a), count(b), count(c), count(d), count(distinct a), count(distinct b), count(distinct c), count(distinct d), count(distinct a,b), count(distinct b,c), count(distinct c,d), count(distinct a,d), count(distinct a,c), count(distinct b,d), count(distinct a,b,c), count(distinct b,c,d), count(distinct a,c,d), count(distinct a,b,d), count(distinct a,b,c,d) from abcd;
select count(1), count(*), count(a), count(b), count(c), count(d), count(distinct a), count(distinct b), count(distinct c), count(distinct d), count(distinct a,b), count(distinct b,c), count(distinct c,d), count(distinct a,d), count(distinct a,c), count(distinct b,d), count(distinct a,b,c), count(distinct b,c,d), count(distinct a,c,d), count(distinct a,b,d), count(distinct a,b,c,d) from abcd;
