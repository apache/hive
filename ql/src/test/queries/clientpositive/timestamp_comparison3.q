set hive.fetch.task.conversion=none;
set hive.stats.fetch.column.stats=true;

create database timestamp_test_n123;
create table timestamp_test_n123.onecolumntable (ts timestamp);

insert into timestamp_test_n123.onecolumntable values
('2015-01-01 00:00:00'),
('2015-01-02 00:00:00'),
('2015-01-03 00:00:00'),
('2015-01-04 00:00:00'),
('2015-01-05 00:00:00');

describe formatted timestamp_test_n123.onecolumntable ts;

explain
select ts from timestamp_test_n123.onecolumntable
where ts >= cast('2015-01-02 00:00:00' as timestamp)
  and ts <= cast('2015-01-04 00:00:00' as timestamp);

explain
select ts from timestamp_test_n123.onecolumntable
where ts >= cast('2015-01-02 00:00:00' as timestamp)
  and ts <= cast('2015-01-03 00:00:00' as timestamp);

explain
select ts from timestamp_test_n123.onecolumntable
where ts >= cast('2015-01-01 00:00:00' as timestamp)
  and ts <= cast('2015-01-08 00:00:00' as timestamp);

drop table timestamp_test_n123.onecolumntable;
drop database timestamp_test_n123;
