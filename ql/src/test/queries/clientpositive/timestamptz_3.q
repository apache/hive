set hive.fetch.task.conversion=more;

drop table tstz1_n1;

create table tstz1_n1(t timestamp with local time zone);

insert overwrite table tstz1_n1 select cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);

select cast(t as timestamp) from tstz1_n1;
select cast(to_epoch_milli(t) as timestamp) from tstz1_n1;

set time zone UTC;

select cast(t as timestamp) from tstz1_n1;
select cast(to_epoch_milli(t) as timestamp) from tstz1_n1;
