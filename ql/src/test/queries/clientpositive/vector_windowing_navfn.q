--! qt:dataset:src
set hive.explain.user=false;
set hive.cli.print.header=true;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.ptf.enabled=true;
set hive.fetch.task.conversion=none;

drop table over10k_n7;

create table over10k_n7(
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
       fields terminated by '|'
       TBLPROPERTIES ("hive.serialization.decode.binary.as.base64"="false");

load data local inpath '../../data/files/over10k' into table over10k_n7;

explain vectorization detail
select row_number() over()  from src where key = '238';

select row_number() over()  from src where key = '238';

explain vectorization detail
select s, row_number() over (partition by d order by `dec`) from over10k_n7 limit 100;

select s, row_number() over (partition by d order by `dec`) from over10k_n7 limit 100;

explain vectorization detail
select i, lead(s) over (partition by bin order by d,i desc) from over10k_n7 limit 100;

select i, lead(s) over (partition by bin order by d,i desc) from over10k_n7 limit 100;

explain vectorization detail
select i, lag(`dec`) over (partition by i order by s,i,`dec`) from over10k_n7 limit 100;

select i, lag(`dec`) over (partition by i order by s,i,`dec`) from over10k_n7 limit 100;

explain vectorization detail
select s, last_value(t) over (partition by d order by f) from over10k_n7 limit 100;

select s, last_value(t) over (partition by d order by f) from over10k_n7 limit 100;

explain vectorization detail
select s, first_value(s) over (partition by bo order by s) from over10k_n7 limit 100;

select s, first_value(s) over (partition by bo order by s) from over10k_n7 limit 100;

explain vectorization detail
select t, s, i, last_value(i) over (partition by t order by s) 
from over10k_n7 where (s = 'oscar allen' or s = 'oscar carson') and t = 10;

-- select t, s, i, last_value(i) over (partition by t order by s) 
-- from over10k_n7 where (s = 'oscar allen' or s = 'oscar carson') and t = 10;

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

explain vectorization detail
select a, b,
first_value(b) over (partition by a order by b rows between 1 preceding and 1 following ) ,
first_value(b, true) over (partition by a order by b rows between 1 preceding and 1 following ) ,
first_value(b) over (partition by a order by b rows between unbounded preceding and 1 following ) ,
first_value(b, true) over (partition by a order by b rows between unbounded preceding and 1 following ) 
from wtest;

select a, b,
first_value(b) over (partition by a order by b rows between 1 preceding and 1 following ) ,
first_value(b, true) over (partition by a order by b rows between 1 preceding and 1 following ) ,
first_value(b) over (partition by a order by b rows between unbounded preceding and 1 following ) ,
first_value(b, true) over (partition by a order by b rows between unbounded preceding and 1 following ) 
from wtest;

explain vectorization detail
select a, b,
first_value(b) over (partition by a order by b desc  rows between 1 preceding and 1 following ) ,
first_value(b, true) over (partition by a order by b desc rows between 1 preceding and 1 following ) ,
first_value(b) over (partition by a order by b desc rows between unbounded preceding and 1 following ) ,
first_value(b, true) over (partition by a order by b desc rows between unbounded preceding and 1 following ) 
from wtest;

select a, b,
first_value(b) over (partition by a order by b desc  rows between 1 preceding and 1 following ) ,
first_value(b, true) over (partition by a order by b desc rows between 1 preceding and 1 following ) ,
first_value(b) over (partition by a order by b desc rows between unbounded preceding and 1 following ) ,
first_value(b, true) over (partition by a order by b desc rows between unbounded preceding and 1 following ) 
from wtest;

explain vectorization detail
select a, b,
last_value(b) over (partition by a order by b rows between 1 preceding and 1 following ) ,
last_value(b, true) over (partition by a order by b rows between 1 preceding and 1 following ) ,
last_value(b) over (partition by a order by b rows between unbounded preceding and 1 following ) ,
last_value(b, true) over (partition by a order by b rows between unbounded preceding and 1 following ) 
from wtest;

select a, b,
last_value(b) over (partition by a order by b rows between 1 preceding and 1 following ) ,
last_value(b, true) over (partition by a order by b rows between 1 preceding and 1 following ) ,
last_value(b) over (partition by a order by b rows between unbounded preceding and 1 following ) ,
last_value(b, true) over (partition by a order by b rows between unbounded preceding and 1 following ) 
from wtest;

explain vectorization detail
select a, b,
last_value(b) over (partition by a order by b desc  rows between 1 preceding and 1 following ) ,
last_value(b, true) over (partition by a order by b desc rows between 1 preceding and 1 following ) ,
last_value(b) over (partition by a order by b desc rows between unbounded preceding and 1 following ) ,
last_value(b, true) over (partition by a order by b desc rows between unbounded preceding and 1 following ) 
from wtest;

select a, b,
last_value(b) over (partition by a order by b desc  rows between 1 preceding and 1 following ) ,
last_value(b, true) over (partition by a order by b desc rows between 1 preceding and 1 following ) ,
last_value(b) over (partition by a order by b desc rows between unbounded preceding and 1 following ) ,
last_value(b, true) over (partition by a order by b desc rows between unbounded preceding and 1 following ) 
from wtest;
