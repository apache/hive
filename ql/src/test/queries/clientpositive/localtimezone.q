drop table `date_test`;
drop table `timestamp_test`;
drop table `timestamptz_test`;

create table `date_test` (`mydate1` date);

insert into `date_test` VALUES
  ('2011-01-01 01:01:01.123'),
  ('2011-01-01 01:01:01.123 Europe/Rome'),
  ('2011-01-01 01:01:01.123 GMT-05:00'),
  ('2011-01-01 01:01:01.12345678912'),
  ('2011-01-01 01:01:01.12345678912 Europe/Rome'),
  ('2011-01-01 01:01:01.12345678912 GMT-05:00'),
  ('2011-01-01 01:01:01.12345678912 xyz');

create table `timestamp_test` (`mydate1` timestamp);

insert into `timestamp_test` VALUES
  ('2011-01-01 01:01:01.123'),
  ('2011-01-01 01:01:01.123 Europe/Rome'),
  ('2011-01-01 01:01:01.123 GMT-05:00'),
  ('2011-01-01 01:01:01.12345678912'),
  ('2011-01-01 01:01:01.12345678912 Europe/Rome'),
  ('2011-01-01 01:01:01.12345678912 GMT-05:00'),
  ('2011-01-01 01:01:01.12345678912 xyz');

create table `timestamptz_test` (`mydate1` timestamp with local time zone);

insert into `timestamptz_test` VALUES
  ('2011-01-01 01:01:01.123'),
  ('2011-01-01 01:01:01.123 Europe/Rome'),
  ('2011-01-01 01:01:01.123 GMT-05:00'),
  ('2011-01-01 01:01:01.12345678912'),
  ('2011-01-01 01:01:01.12345678912 Europe/Rome'),
  ('2011-01-01 01:01:01.12345678912 GMT-05:00'),
  ('2011-01-01 01:01:01.12345678912 xyz');

select * from `date_test`;
select * from `timestamp_test`;
select * from `timestamptz_test`;

set time zone Europe/Rome;

select * from `date_test`;
select * from `timestamp_test`;
select * from `timestamptz_test`;

set hive.local.time.zone=America/Los_Angeles;

select * from `date_test`;
select * from `timestamp_test`;
select * from `timestamptz_test`;

set time  zone GMT-07:00;

select * from `date_test`;
select * from `timestamp_test`;
select * from `timestamptz_test`;

select extract(year from `mydate1`) from `timestamptz_test`;
select extract(quarter from `mydate1`) from `timestamptz_test`;
select extract(month from `mydate1`) from `timestamptz_test`;
select extract(day from `mydate1`) from `timestamptz_test`;
select extract(hour from `mydate1`) from `timestamptz_test`;
select extract(minute from `mydate1`) from `timestamptz_test`;
select extract(second from `mydate1`) from `timestamptz_test`;

select cast(`mydate1` as date) from `timestamptz_test`;
select cast(`mydate1` as timestamp with local time zone) from `date_test`;
select cast(`mydate1` as timestamp) from `timestamptz_test`;
select cast(`mydate1` as timestamp with local time zone) from `timestamp_test`;

select `mydate1` from `timestamptz_test` group by `mydate1`;
select a.`mydate1` as c1, b.`mydate1` as c2
from `timestamptz_test` a join `timestamptz_test` b
on a.`mydate1` = b.`mydate1`;

create table `timestamptz_test2` (`mydate1` timestamp with local time zone, `item` string, `price` double);
insert into `timestamptz_test2` VALUES
  ('2011-01-01 01:01:01.123', 'laptop 1', 9.2),
  ('2011-01-01 01:01:01.123', 'mouse 1', 3.1),
  ('2011-01-01 01:01:01.123 Europe/Rome', 'keyboard 1', 4.2),
  ('2011-01-01 01:01:01.123 GMT-05:00', 'keyboard 2', 3.9),
  ('2011-01-01 01:01:01.123 GMT-05:00', 'keyboard 3', 3.99),
  ('2011-01-01 01:01:01.12345678912', 'mouse 2', 4.594),
  ('2011-01-01 01:01:01.12345678912 Europe/Rome', 'laptop 2', 10),
  ('2011-01-01 01:01:01.12345678912 Europe/Rome', 'hdmi', 1.25),
  ('2011-01-01 01:01:01.12345678912 GMT-05:00', 'pin', null),
  ('2011-01-01 01:01:01.12345678912 xyz', 'cable', 0.0);
select `item`, `price`,
rank() over (partition by `mydate1` order by `price`) as r
from `timestamptz_test2`;
select `item`, `price`,
rank() over (partition by cast(`mydate1` as date) order by `price`) as r
from `timestamptz_test2`;