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

select s, row_number() over (partition by d order by dec) from over10k limit 100;

select i, lead(s) over (partition by bin order by d,i desc) from over10k limit 100;

select i, lag(dec) over (partition by i order by s,i,dec) from over10k limit 100;

select s, last_value(t) over (partition by d order by f) from over10k limit 100;

select s, first_value(s) over (partition by bo order by s) from over10k limit 100;

select t, s, i, last_value(i) over (partition by t order by s) 
from over10k where (s = 'oscar allen' or s = 'oscar carson') and t = 10;

drop table if exists wtest;
create table wtest as
select a, b
from
(
SELECT explode(
   map(
   3, array(1,2,3,4,5), 
   1, array(int(null),int(null),int(null), int(null), int(null)), 
   2, array(1,null,2, null, 3)
   )
  ) as (a,barr) FROM (select * from src limit 1) s
  ) s1 lateral view explode(barr) arr as b;
  
select a, b,
first_value(b) over (partition by a order by b rows between 1 preceding and 1 following ) ,
first_value(b, true) over (partition by a order by b rows between 1 preceding and 1 following ) ,
first_value(b) over (partition by a order by b rows between unbounded preceding and 1 following ) ,
first_value(b, true) over (partition by a order by b rows between unbounded preceding and 1 following ) 
from wtest;


select a, b,
first_value(b) over (partition by a order by b desc  rows between 1 preceding and 1 following ) ,
first_value(b, true) over (partition by a order by b desc rows between 1 preceding and 1 following ) ,
first_value(b) over (partition by a order by b desc rows between unbounded preceding and 1 following ) ,
first_value(b, true) over (partition by a order by b desc rows between unbounded preceding and 1 following ) 
from wtest;

select a, b,
last_value(b) over (partition by a order by b rows between 1 preceding and 1 following ) ,
last_value(b, true) over (partition by a order by b rows between 1 preceding and 1 following ) ,
last_value(b) over (partition by a order by b rows between unbounded preceding and 1 following ) ,
last_value(b, true) over (partition by a order by b rows between unbounded preceding and 1 following ) 
from wtest;

select a, b,
last_value(b) over (partition by a order by b desc  rows between 1 preceding and 1 following ) ,
last_value(b, true) over (partition by a order by b desc rows between 1 preceding and 1 following ) ,
last_value(b) over (partition by a order by b desc rows between unbounded preceding and 1 following ) ,
last_value(b, true) over (partition by a order by b desc rows between unbounded preceding and 1 following ) 
from wtest;