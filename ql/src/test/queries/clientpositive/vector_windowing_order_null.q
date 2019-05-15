set hive.explain.user=false;
set hive.cli.print.header=true;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.ptf.enabled=true;
set hive.fetch.task.conversion=none;

drop table over10k_n21;

create table over10k_n21(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
	   ts timestamp, 
           `dec` decimal,  
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/over10k' into table over10k_n21;
load data local inpath '../../data/files/over4_null' into table over10k_n21;

explain vectorization detail
select i, s, b, sum(b) over (partition by i order by s nulls last,b rows unbounded preceding) from over10k_n21 limit 10;
select i, s, b, sum(b) over (partition by i order by s nulls last,b rows unbounded preceding) from over10k_n21 limit 10;

explain vectorization detail
select d, s, f, sum(f) over (partition by d order by s,f desc nulls first rows unbounded preceding) from over10k_n21 limit 10;
select d, s, f, sum(f) over (partition by d order by s,f desc nulls first rows unbounded preceding) from over10k_n21 limit 10;

explain vectorization detail
select ts, s, f, sum(f) over (partition by ts order by f asc nulls first range between current row and unbounded following) from over10k_n21 limit 10;
select ts, s, f, sum(f) over (partition by ts order by f asc nulls first range between current row and unbounded following) from over10k_n21 limit 10;

explain vectorization detail
select t, s, d, avg(d) over (partition by t order by s,d desc nulls first rows between 5 preceding and 5 following) from over10k_n21 limit 10;
select t, s, d, avg(d) over (partition by t order by s,d desc nulls first rows between 5 preceding and 5 following) from over10k_n21 limit 10;

explain vectorization detail
select ts, s, sum(i) over(partition by ts order by s nulls last) from over10k_n21 limit 10 offset 3;
select ts, s, sum(i) over(partition by ts order by s nulls last) from over10k_n21 limit 10 offset 3;

explain vectorization detail
select s, i, round(sum(d) over (partition by s order by i desc nulls last) , 3) from over10k_n21 limit 5;
select s, i, round(sum(d) over (partition by s order by i desc nulls last) , 3) from over10k_n21 limit 5;

explain vectorization detail
select s, i, round(avg(d) over (partition by s order by i desc nulls last) / 10.0 , 3) from over10k_n21 limit 5;
select s, i, round(avg(d) over (partition by s order by i desc nulls last) / 10.0 , 3) from over10k_n21 limit 5;

explain vectorization detail
select s, i, round((avg(d) over  w1 + 10.0) - (avg(d) over w1 - 10.0),3) from over10k_n21 window w1 as (partition by s order by i nulls last) limit 5;
select s, i, round((avg(d) over  w1 + 10.0) - (avg(d) over w1 - 10.0),3) from over10k_n21 window w1 as (partition by s order by i nulls last) limit 5;
