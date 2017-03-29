set hive.parquet.timestamp.skip.conversion=true;

create table timestamps (ts timestamp) stored as parquet;
insert into table timestamps select cast('2016-01-01 01:01:01' as timestamp) limit 1;
select * from timestamps;
drop table timestamps;

set hive.parquet.timestamp.skip.conversion=false;

create table timestamps (ts timestamp) stored as parquet;
insert into table timestamps select cast('2017-01-01 01:01:01' as timestamp) limit 1;
select * from timestamps;
drop table timestamps;