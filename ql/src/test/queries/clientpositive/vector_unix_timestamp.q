
create table t (d string);
insert into t values('1400-11-16 22:18:40 UTC');

select 
  '>' || d || '<' , unix_timestamp(d,"yyyy-MM-dd HH:mm:ss z"), from_unixtime(unix_timestamp(d,"yyyy-MM-dd HH:mm:ss z")), to_date(from_unixtime(unix_timestamp(d,"yyyy-MM-dd HH:mm:ss z")))
from t
;

set hive.fetch.task.conversion=none;

select 
  '>' || d || '<' , unix_timestamp(d,"yyyy-MM-dd HH:mm:ss z"), from_unixtime(unix_timestamp(d,"yyyy-MM-dd HH:mm:ss z")), to_date(from_unixtime(unix_timestamp(d,"yyyy-MM-dd HH:mm:ss z")))
from t
;

select 
  '>' || d || '<' , assert_true(unix_timestamp(d,"yyyy-MM-dd HH:mm:ss z") is not null)
from t
;

create table t1 (d string);
insert into t1 values('1400-11-16 22:18:40');

select
  '>' || d || '<' , unix_timestamp(d), from_unixtime(unix_timestamp(d)), to_date(from_unixtime(unix_timestamp(d)))
from t1
;

set hive.fetch.task.conversion=none;

select
  '>' || d || '<' , unix_timestamp(d), from_unixtime(unix_timestamp(d)), to_date(from_unixtime(unix_timestamp(d)))
from t1
;

select
  '>' || d || '<' , assert_true(unix_timestamp(d) is not null)
from t1
;

