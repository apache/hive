
set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

explain
select ctimestamp1, unix_timestamp(ctimestamp1), to_unix_timestamp(ctimestamp1) from alltypesorc limit 1;

select ctimestamp1, unix_timestamp(ctimestamp1), to_unix_timestamp(ctimestamp1) from alltypesorc limit 1;

create temporary table src1orc stored as orc as select * from src1;

explain
select from_unixtime(to_unix_timestamp(ctimestamp1), 'EEEE') from alltypesorc limit 1; 

select from_unixtime(to_unix_timestamp(ctimestamp1), 'EEEE') from alltypesorc limit 1; 

explain
select from_unixtime(unix_timestamp(ctimestamp1), 'EEEE') from alltypesorc limit 1; 

select from_unixtime(unix_timestamp(ctimestamp1), 'EEEE') from alltypesorc limit 1; 
