set hive.fetch.task.conversion=more;

create table tstz4(t timestamp);

insert into tstz4 VALUES ('2013-06-03 02:01:00.30547 GMT+01:00'), ('2013-06-03 02:01:00.30547 America/Los_Angeles'), ('2013-06-03 02:01:00.30547+01:00'), ('2013-06-03 02:01:00 GMT+01:00'), ('2013-06-03 02:01:00+07:00'), ('2013-06-03 02:01:00 America/Los_Angeles');

select * from tstz4;

