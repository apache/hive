set hive.cli.print.header=true;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.ptf.enabled=true;
set hive.fetch.task.conversion=none;

drop table over10k_n5;

create table over10k_n5(
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

load data local inpath '../../data/files/over10k' into table over10k_n5;

explain vectorization detail
select first_value(t) over ( partition by si order by i, b ) from over10k_n5 limit 100;
select first_value(t) over ( partition by si order by i, b ) from over10k_n5 limit 100;

explain vectorization detail
select last_value(i) over (partition by si, bo order by i, f desc range current row) from over10k_n5 limit 100;
select last_value(i) over (partition by si, bo order by i, f desc range current row) from over10k_n5 limit 100;

explain vectorization detail
select row_number() over (partition by si, bo order by i, f desc range between unbounded preceding and unbounded following) from over10k_n5 limit 100;
select row_number() over (partition by si, bo order by i, f desc range between unbounded preceding and unbounded following) from over10k_n5 limit 100;

explain vectorization detail
select s, si, i, avg(i) over (partition by s range between unbounded preceding and current row) from over10k_n5;
select s, si, i, avg(i) over (partition by s range between unbounded preceding and current row) from over10k_n5;

explain vectorization detail
select s, si, i, avg(i) over (partition by s order by si, i range between unbounded preceding and current row) from over10k_n5 limit 100;
select s, si, i, avg(i) over (partition by s order by si, i range between unbounded preceding and current row) from over10k_n5 limit 100;

explain vectorization detail
select s, si, i, min(i) over (partition by s order by si, i range between unbounded preceding and current row) from over10k_n5 limit 100;
select s, si, i, min(i) over (partition by s order by si, i range between unbounded preceding and current row) from over10k_n5 limit 100;

explain vectorization detail
select s, si, i, avg(i) over (partition by s order by si, i desc range between unbounded preceding and current row) from over10k_n5 limit 100;
select s, si, i, avg(i) over (partition by s order by si, i desc range between unbounded preceding and current row) from over10k_n5 limit 100;

explain vectorization detail
select si, bo, i, f, max(i) over (partition by si, bo order by i, f desc range between unbounded preceding and current row) from over10k_n5 limit 100;
select si, bo, i, f, max(i) over (partition by si, bo order by i, f desc range between unbounded preceding and current row) from over10k_n5 limit 100;

explain vectorization detail
select bo, rank() over (partition by i order by bo nulls first, b nulls last range between unbounded preceding and unbounded following) from over10k_n5 limit 100;
select bo, rank() over (partition by i order by bo nulls first, b nulls last range between unbounded preceding and unbounded following) from over10k_n5 limit 100;

explain vectorization detail
select CAST(s as CHAR(12)), rank() over (partition by i order by CAST(s as CHAR(12)) nulls last range between unbounded preceding and unbounded following) from over10k_n5 limit 100;
select CAST(s as CHAR(12)), rank() over (partition by i order by CAST(s as CHAR(12)) nulls last range between unbounded preceding and unbounded following) from over10k_n5 limit 100;

explain vectorization detail
select CAST(s as VARCHAR(12)), rank() over (partition by i order by CAST(s as VARCHAR(12)) nulls last range between unbounded preceding and unbounded following) from over10k_n5 limit 100;
select CAST(s as VARCHAR(12)), rank() over (partition by i order by CAST(s as VARCHAR(12)) nulls last range between unbounded preceding and unbounded following) from over10k_n5 limit 100;
