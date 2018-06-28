drop table `table_tsltz`;

CREATE TABLE table_tsltz (tz VARCHAR(200),
                         c_ts1 TIMESTAMP,
                         c_ts2 TIMESTAMP,
                         c_tsltz1 TIMESTAMP WITH LOCAL TIME ZONE,
                         c_tsltz2 TIMESTAMP WITH LOCAL TIME ZONE);

set time zone GMT-08:00;

insert into table_tsltz values (
  '-08:00',
  cast('2016-01-01 00:00:00' as timestamp),
  cast('2016-01-01 00:00:00 -05:00' as timestamp with local time zone),
  cast('2016-01-01 00:00:00' as timestamp),
  cast('2016-01-01 00:00:00 -05:00' as timestamp with local time zone));

set time zone UTC;

insert into table_tsltz values (
  'UTC',
  cast('2016-01-01 00:00:00' as timestamp),
  cast('2016-01-01 00:00:00 -05:00' as timestamp with local time zone),
  cast('2016-01-01 00:00:00' as timestamp),
  cast('2016-01-01 00:00:00 -05:00' as timestamp with local time zone));

set time zone GMT+02:00;

insert into table_tsltz values (
  '+02:00',
  cast('2016-01-01 00:00:00' as timestamp),
  cast('2016-01-01 00:00:00 -05:00' as timestamp with local time zone),
  cast('2016-01-01 00:00:00' as timestamp),
  cast('2016-01-01 00:00:00 -05:00' as timestamp with local time zone));

set time zone US/Pacific;

insert into table_tsltz values (
  'US/Pacific',
  cast('2016-01-01 00:00:00' as timestamp),
  cast('2016-01-01 00:00:00 -05:00' as timestamp with local time zone),
  cast('2016-01-01 00:00:00' as timestamp),
  cast('2016-01-01 00:00:00 -05:00' as timestamp with local time zone));

select tz,
    c_ts1, c_ts2,
    cast(c_tsltz1 as VARCHAR(200)) as c_tsltz1, cast(c_tsltz2 as VARCHAR(200)) as c_tsltz2
from table_tsltz;

set time zone UTC;

select tz,
    c_ts1, c_ts2,
    cast(c_tsltz1 as VARCHAR(200)) as c_tsltz1, cast(c_tsltz2 as VARCHAR(200)) as c_tsltz2
from table_tsltz;
