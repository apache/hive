set hive.cbo.enable=false;

drop table over10k_n14;

create table over10k_n14(
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

load data local inpath '../../data/files/over10k' into table over10k_n14;

explain
select distinct first_value(t) over ( partition by si order by i ) from over10k_n14 limit 10;

select distinct first_value(t) over ( partition by si order by i ) from over10k_n14 limit 10;

explain
select distinct last_value(i) over ( partition by si order by i )
from over10k_n14 limit 10;

select distinct last_value(i) over ( partition by si order by i )
from over10k_n14 limit 10;

explain
select distinct last_value(i) over ( partition by si order by i ),
                first_value(t)  over ( partition by si order by i )
from over10k_n14 limit 50;

select distinct last_value(i) over ( partition by si order by i ),
                first_value(t)  over ( partition by si order by i )
from over10k_n14 limit 50;

explain
select si, max(f) mf, rank() over ( partition by si order by mf )
FROM over10k_n14
GROUP BY si
HAVING max(f) > 0
limit 50;

select si, max(f) mf, rank() over ( partition by si order by mf )
FROM over10k_n14
GROUP BY si
HAVING max(f) > 0
limit 50;

explain
select distinct si, rank() over ( partition by si order by i )
FROM over10k_n14
limit 50;

select distinct si, rank() over ( partition by si order by i )
FROM over10k_n14
limit 50;
