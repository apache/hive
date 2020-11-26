
create table t (meta_createddate string);
insert into t values('2020-11-16 22:18:40 UTC');
set hive.fetch.task.conversion=none;
select 
  meta_createddate,'x',
  meta_createddate is null,
  '>' || meta_createddate || '<' ,
   unix_timestamp(meta_createddate),
from_unixtime(unix_timestamp(meta_createddate)),
  to_date(from_unixtime(unix_timestamp(meta_createddate)))
from t
;
