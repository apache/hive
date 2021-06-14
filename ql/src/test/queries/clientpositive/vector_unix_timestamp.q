
create table t (d string);
insert into t values('2020-11-16 22:18:40 UTC');

select 
  '>' || d || '<' , to_unix_timestamp(d), from_unixtime(to_unix_timestamp(d)), to_date(from_unixtime(to_unix_timestamp(d)))
from t
;

set hive.fetch.task.conversion=none;

select 
  '>' || d || '<' , to_unix_timestamp(d), from_unixtime(to_unix_timestamp(d)), to_date(from_unixtime(to_unix_timestamp(d)))
from t
;

select 
  '>' || d || '<' , assert_true(to_unix_timestamp(d) is not null)
from t
;

