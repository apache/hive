set hive.fetch.task.conversion=more;

drop table tstz1;

create table tstz1(t timestamp with local time zone);

insert overwrite table tstz1 select cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);
select cast(t as string) from tstz1;
select cast(t as date) from tstz1;
select cast(t as timestamp) from tstz1;

insert overwrite table tstz1 select '2016-01-03 12:26:34.1 GMT';
select cast(t as string) from tstz1;
select cast(t as date) from tstz1;
select cast(t as timestamp) from tstz1;

insert overwrite table tstz1 select '2016-01-03 12:26:34.0123 Europe/London';
select cast(t as string) from tstz1;
select cast(t as date) from tstz1;
select cast(t as timestamp) from tstz1;

insert overwrite table tstz1 select '2016-01-03 12:26:34.012300 GMT+08:00';
select cast(t as string) from tstz1;
select cast(t as date) from tstz1;
select cast(t as timestamp) from tstz1;
