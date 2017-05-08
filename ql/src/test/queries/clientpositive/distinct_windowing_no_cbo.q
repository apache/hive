set hive.cbo.enable=false;

drop table over10k;

create table over10k(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           dec decimal(4,2),
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/over10k' into table over10k;

explain
select distinct first_value(t) over ( partition by si order by i ) from over10k limit 10;

select distinct first_value(t) over ( partition by si order by i ) from over10k limit 10;

explain
select distinct last_value(i) over ( partition by si order by i )
from over10k limit 10;

select distinct last_value(i) over ( partition by si order by i )
from over10k limit 10;

explain
select distinct last_value(i) over ( partition by si order by i ),
                first_value(t)  over ( partition by si order by i )
from over10k limit 50;

select distinct last_value(i) over ( partition by si order by i ),
                first_value(t)  over ( partition by si order by i )
from over10k limit 50;

explain
select si, max(f) mf, rank() over ( partition by si order by mf )
FROM over10k
GROUP BY si
HAVING max(f) > 0
limit 50;

select si, max(f) mf, rank() over ( partition by si order by mf )
FROM over10k
GROUP BY si
HAVING max(f) > 0
limit 50;

explain
select distinct si, rank() over ( partition by si order by i )
FROM over10k
limit 50;

select distinct si, rank() over ( partition by si order by i )
FROM over10k
limit 50;
