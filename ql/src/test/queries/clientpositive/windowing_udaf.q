--! qt:dataset:src1
drop table over10k_n4;

create table over10k_n4(
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

load data local inpath '../../data/files/over10k' into table over10k_n4;

select s, min(i) over (partition by s) m from over10k_n4 order by s, m limit 100;

select s, avg(f) over (partition by si order by s) a from over10k_n4 order by s, a limit 100;

select s, avg(i) over (partition by t, b order by s) a from over10k_n4 order by s, a limit 100;

select max(i) over w m from over10k_n4 window w as (partition by f) order by m limit 100;

select s, avg(d) over (partition by t order by f) a from over10k_n4 order by s, a limit 100;

select key, max(value) over
  (order by key rows between 10 preceding and 20 following) m
from src1 where length(key) > 10
order by key, m;
