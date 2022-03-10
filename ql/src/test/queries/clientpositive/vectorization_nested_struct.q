create table t as
select
named_struct('id',13,'str','string','nest',named_struct('id',12,'str','string','arr',array('value','value','value','value','value','value','value','value','value','value','value','value','value','value','value','value','value','value','value','value','value','value','value','value','value','value','value','value','value','value','value','value')))
s;

-- go up to 100K rows
insert into table t select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t;
insert into table t select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t;
insert into table t select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t;
insert into table t select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t;
insert into table t select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t union all select * from t;

set hive.fetch.task.conversion=none;

select count(1)
from t
where 
s
.nest
.id  > 0;

