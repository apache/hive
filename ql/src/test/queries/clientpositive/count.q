set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
-- SORT_QUERY_RESULTS
create table abcd_n2 (a int, b int, c int, d int);
LOAD DATA LOCAL INPATH '../../data/files/in4.txt' INTO TABLE abcd_n2;

select * from abcd_n2;
set hive.map.aggr=true;
explain select a, count(distinct b), count(distinct c), sum(d) from abcd_n2 group by a;
select a, count(distinct b), count(distinct c), sum(d) from abcd_n2 group by a;

explain select count(1), count(*), count(a), count(b), count(c), count(d), count(distinct a), count(distinct b), count(distinct c), count(distinct d), count(distinct a,b), count(distinct b,c), count(distinct c,d), count(distinct a,d), count(distinct a,c), count(distinct b,d), count(distinct a,b,c), count(distinct b,c,d), count(distinct a,c,d), count(distinct a,b,d), count(distinct a,b,c,d) from abcd_n2;
select count(1), count(*), count(a), count(b), count(c), count(d), count(distinct a), count(distinct b), count(distinct c), count(distinct d), count(distinct a,b), count(distinct b,c), count(distinct c,d), count(distinct a,d), count(distinct a,c), count(distinct b,d), count(distinct a,b,c), count(distinct b,c,d), count(distinct a,c,d), count(distinct a,b,d), count(distinct a,b,c,d) from abcd_n2;

set hive.map.aggr=false;
explain select a, count(distinct b), count(distinct c), sum(d) from abcd_n2 group by a;
select a, count(distinct b), count(distinct c), sum(d) from abcd_n2 group by a;

explain select count(1), count(*), count(a), count(b), count(c), count(d), count(distinct a), count(distinct b), count(distinct c), count(distinct d), count(distinct a,b), count(distinct b,c), count(distinct c,d), count(distinct a,d), count(distinct a,c), count(distinct b,d), count(distinct a,b,c), count(distinct b,c,d), count(distinct a,c,d), count(distinct a,b,d), count(distinct a,b,c,d) from abcd_n2;
select count(1), count(*), count(a), count(b), count(c), count(d), count(distinct a), count(distinct b), count(distinct c), count(distinct d), count(distinct a,b), count(distinct b,c), count(distinct c,d), count(distinct a,d), count(distinct a,c), count(distinct b,d), count(distinct a,b,c), count(distinct b,c,d), count(distinct a,c,d), count(distinct a,b,d), count(distinct a,b,c,d) from abcd_n2;

set hive.cbo.returnpath.hiveop=true;

set hive.map.aggr=true;
--first aggregation with literal. gbinfo was generating wrong expression
explain select count(1), count(*), count(a), count(b), count(c), count(d), count(distinct a), count(distinct b), count(distinct c), count(distinct d), count(distinct a,b), count(distinct b,c), count(distinct c,d), count(distinct a,d), count(distinct a,c), count(distinct b,d), count(distinct a,b,c), count(distinct b,c,d), count(distinct a,c,d), count(distinct a,b,d), count(distinct a,b,c,d) from abcd_n2;
select count(1), count(*), count(a), count(b), count(c), count(d), count(distinct a), count(distinct b), count(distinct c), count(distinct d), count(distinct a,b), count(distinct b,c), count(distinct c,d), count(distinct a,d), count(distinct a,c), count(distinct b,d), count(distinct a,b,c), count(distinct b,c,d), count(distinct a,c,d), count(distinct a,b,d), count(distinct a,b,c,d) from abcd_n2;

set hive.map.aggr=false;
explain select count(distinct b) from abcd_n2 group by a;
select count(distinct b) from abcd_n2 group by a;

explain select count(distinct b) from abcd_n2 group by b;
select count(distinct b) from abcd_n2 group by b;

explain select count(distinct b) from abcd_n2 group by c;
select count(distinct b) from abcd_n2 group by c;

explain select count(b), count(distinct c) from abcd_n2 group by d;
select count(b), count(distinct c) from abcd_n2 group by d;

--non distinct aggregate with same column as group by key
explain select a, count(distinct b), count(distinct c), sum(d), sum(d+d), sum(d*3), sum(b), sum(c), sum(a), sum(distinct a), sum(distinct b) from abcd_n2 group by a;
select a, count(distinct b), count(distinct c), sum(d), sum(d+d), sum(d*3), sum(b), sum(c), sum(a), sum(distinct a), sum(distinct b) from abcd_n2 group by a;

--non distinct aggregate with same column as distinct aggregate
explain select a, count(distinct b), count(distinct c), sum(d), sum(c) from abcd_n2 group by a;
select a, count(distinct b), count(distinct c), sum(d), sum(c) from abcd_n2 group by a;

--aggregation with literal
explain select count(1), count(*), count(a), count(b), count(c), count(d), count(distinct a), count(distinct b), count(distinct c), count(distinct d), count(distinct a,b), count(distinct b,c), count(distinct c,d), count(distinct a,d), count(distinct a,c), count(distinct b,d), count(distinct a,b,c), count(distinct b,c,d), count(distinct a,c,d), count(distinct a,b,d), count(distinct a,b,c,d) from abcd_n2;
select count(1), count(*), count(a), count(b), count(c), count(d), count(distinct a), count(distinct b), count(distinct c), count(distinct d), count(distinct a,b), count(distinct b,c), count(distinct c,d), count(distinct a,d), count(distinct a,c), count(distinct b,d), count(distinct a,b,c), count(distinct b,c,d), count(distinct a,c,d), count(distinct a,b,d), count(distinct a,b,c,d) from abcd_n2;

set hive.cbo.returnpath.hiveop=false;
