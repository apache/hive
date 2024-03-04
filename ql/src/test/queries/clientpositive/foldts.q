
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

select from_unixtime(to_unix_timestamp(ctimestamp1)) from alltypesorc limit 1; 

select from_unixtime(unix_timestamp(ctimestamp1) ,"yyyy-MM-dd'T'HH:mm:ssXXX") from alltypesorc limit 1;

select from_unixtime(unix_timestamp(ctimestamp1) ,"uuuu-MM-dd'T'HH:mm:ssXXX") from alltypesorc limit 1;

set time zone Europe/Rome;

select from_unixtime(to_unix_timestamp(ctimestamp1)) from alltypesorc limit 1;

select from_unixtime(unix_timestamp(ctimestamp1) ,"yyyy-MM-dd'T'HH:mm:ssXXX") from alltypesorc limit 1;

select from_unixtime(unix_timestamp(ctimestamp1) ,"uuuu-MM-dd'T'HH:mm:ssXXX") from alltypesorc limit 1;

select from_unixtime(to_unix_timestamp(cast(cast(ctimestamp1 as string) || ' America/Los_Angeles' as timestamp with local time zone))) from alltypesorc limit 1;

select from_unixtime(to_unix_timestamp(cast(cast(ctimestamp1 as string) || ' America/Los_Angeles' as timestamp with local time zone)) ,"yyyy-MM-dd'T'HH:mm:ssXXX") from alltypesorc limit 1;

select from_unixtime(to_unix_timestamp(cast(cast(ctimestamp1 as string) || ' America/Los_Angeles' as timestamp with local time zone)) ,"uuuu-MM-dd'T'HH:mm:ssXXX") from alltypesorc limit 1;
