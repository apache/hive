set hive.stats.column.autogather=false;
set hive.cli.print.header=true;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.ptf.enabled=true;
set hive.fetch.task.conversion=none;

drop table over10k_n3;

create table over10k_n3(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
	   ts timestamp, 
           `dec` decimal(4,2),  
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/over10k' into table over10k_n3;

explain vectorization detail
select p_mfgr, p_retailprice, p_size,
round(sum(p_retailprice) over w1 , 2) = round(sum(lag(p_retailprice,1,0.0)) over w1 + last_value(p_retailprice) over w1 , 2), 
max(p_retailprice) over w1 - min(p_retailprice) over w1 = last_value(p_retailprice) over w1 - first_value(p_retailprice) over w1
from part
window w1 as (distribute by p_mfgr sort by p_retailprice)
;
select p_mfgr, p_retailprice, p_size,
round(sum(p_retailprice) over w1 , 2) = round(sum(lag(p_retailprice,1,0.0)) over w1 + last_value(p_retailprice) over w1 , 2), 
max(p_retailprice) over w1 - min(p_retailprice) over w1 = last_value(p_retailprice) over w1 - first_value(p_retailprice) over w1
from part
window w1 as (distribute by p_mfgr sort by p_retailprice)
;

explain vectorization detail
select p_mfgr, p_retailprice, p_size,
rank() over (distribute by p_mfgr sort by p_retailprice) as r,
sum(p_retailprice) over (distribute by p_mfgr sort by p_retailprice rows between unbounded preceding and current row) as s2,
sum(p_retailprice) over (distribute by p_mfgr sort by p_retailprice rows between unbounded preceding and current row) -5 as s1
from part
;
select p_mfgr, p_retailprice, p_size,
rank() over (distribute by p_mfgr sort by p_retailprice) as r,
sum(p_retailprice) over (distribute by p_mfgr sort by p_retailprice rows between unbounded preceding and current row) as s2,
sum(p_retailprice) over (distribute by p_mfgr sort by p_retailprice rows between unbounded preceding and current row) -5 as s1
from part
;

explain vectorization detail
select s, si, f, si - lead(f, 3) over (partition by t order by bo,s,si,f desc) from over10k_n3 limit 100;
select s, si, f, si - lead(f, 3) over (partition by t order by bo,s,si,f desc) from over10k_n3 limit 100;
explain vectorization detail
select s, i, i - lead(i, 3, 0) over (partition by si order by i,s) from over10k_n3 limit 100;
select s, i, i - lead(i, 3, 0) over (partition by si order by i,s) from over10k_n3 limit 100;
explain vectorization detail
select s, si, d, si - lag(d, 3) over (partition by b order by si,s,d) from over10k_n3 limit 100;
select s, si, d, si - lag(d, 3) over (partition by b order by si,s,d) from over10k_n3 limit 100;
explain vectorization detail
select s, lag(s, 3, 'fred') over (partition by f order by b) from over10k_n3 limit 100;
select s, lag(s, 3, 'fred') over (partition by f order by b) from over10k_n3 limit 100;

explain vectorization detail
select p_mfgr, avg(p_retailprice) over(partition by p_mfgr, p_type order by p_mfgr) from part;
select p_mfgr, avg(p_retailprice) over(partition by p_mfgr, p_type order by p_mfgr) from part;

explain vectorization detail
select p_mfgr, avg(p_retailprice) over(partition by p_mfgr order by p_type,p_mfgr rows between unbounded preceding and current row) from part;
select p_mfgr, avg(p_retailprice) over(partition by p_mfgr order by p_type,p_mfgr rows between unbounded preceding and current row) from part;

-- multi table insert test
create table t1_n23 (a1 int, b1 string); 
create table t2_n15 (a1 int, b1 string);
explain vectorization detail
from (select sum(i) over (partition by ts order by i), s from over10k_n3) tt insert overwrite table t1_n23 select * insert overwrite table t2_n15 select * ;
from (select sum(i) over (partition by ts order by i), s from over10k_n3) tt insert overwrite table t1_n23 select * insert overwrite table t2_n15 select * ;
select * from t1_n23 limit 3;
select * from t2_n15 limit 3;

explain vectorization detail
select p_mfgr, p_retailprice, p_size,
round(sum(p_retailprice) over w1 , 2) + 50.0 = round(sum(lag(p_retailprice,1,50.0)) over w1 + (last_value(p_retailprice) over w1),2)
from part
window w1 as (distribute by p_mfgr sort by p_retailprice)
limit 11;
select p_mfgr, p_retailprice, p_size,
round(sum(p_retailprice) over w1 , 2) + 50.0 = round(sum(lag(p_retailprice,1,50.0)) over w1 + (last_value(p_retailprice) over w1),2)
from part
window w1 as (distribute by p_mfgr sort by p_retailprice)
limit 11;


--
-- Run some tests with these parameters that force spilling to disk.
--
set hive.vectorized.ptf.max.memory.buffering.batch.count=1;
set hive.vectorized.testing.reducer.batch.size=2;

select p_mfgr, p_retailprice, p_size,
round(sum(p_retailprice) over w1 , 2) = round(sum(lag(p_retailprice,1,0.0)) over w1 + last_value(p_retailprice) over w1 , 2), 
max(p_retailprice) over w1 - min(p_retailprice) over w1 = last_value(p_retailprice) over w1 - first_value(p_retailprice) over w1
from part
window w1 as (distribute by p_mfgr sort by p_retailprice)
;

select p_mfgr, p_retailprice, p_size,
rank() over (distribute by p_mfgr sort by p_retailprice) as r,
sum(p_retailprice) over (distribute by p_mfgr sort by p_retailprice rows between unbounded preceding and current row) as s2,
sum(p_retailprice) over (distribute by p_mfgr sort by p_retailprice rows between unbounded preceding and current row) -5 as s1
from part
;

select p_mfgr, avg(p_retailprice) over(partition by p_mfgr, p_type order by p_mfgr) from part;

select p_mfgr, avg(p_retailprice) over(partition by p_mfgr order by p_type,p_mfgr rows between unbounded preceding and current row) from part;

from (select sum(i) over (partition by ts order by i), s from over10k_n3) tt insert overwrite table t1_n23 select * insert overwrite table t2_n15 select * ;
select * from t1_n23 limit 3;
select * from t2_n15 limit 3;

select p_mfgr, p_retailprice, p_size,
round(sum(p_retailprice) over w1 , 2) + 50.0 = round(sum(lag(p_retailprice,1,50.0)) over w1 + (last_value(p_retailprice) over w1),2)
from part
window w1 as (distribute by p_mfgr sort by p_retailprice)
limit 11;
