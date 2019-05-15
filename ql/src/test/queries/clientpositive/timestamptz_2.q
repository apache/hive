set hive.fetch.task.conversion=more;
set time zone UTC;

drop table tstz2;

create table tstz2(t timestamp with local time zone);

insert into table tstz2 values
  ('2005-04-03 03:01:00.04067 GMT-07:00'),('2005-01-03 02:01:00 GMT'),('2005-01-03 06:01:00 GMT+04:00'),
  ('2013-06-03 02:01:00.30547 GMT+01:00'),('2016-01-03 12:26:34.0123 GMT+08:00');

select * from tstz2 where t='2005-01-02 19:01:00 GMT-07:00';

select * from tstz2 where t>'2013-06-03 02:01:00.30547 GMT+01:00';

select min(t),max(t) from tstz2;

select t from tstz2 group by t order by t;

select * from tstz2 a join tstz2 b on a.t=b.t order by a.t;
