--! qt:dataset:src
CREATE TABLE dest1_n14(c1 STRING) STORED AS TEXTFILE;

FROM src INSERT OVERWRITE TABLE dest1_n14 SELECT '  abc  ' WHERE src.key = 86;

EXPLAIN
SELECT from_unixtime(1226446340), to_date(from_unixtime(1226446340)), day('2008-11-01'), month('2008-11-01'), year('2008-11-01'), day('2008-11-01 15:32:20'), month('2008-11-01 15:32:20'), year('2008-11-01 15:32:20') FROM dest1_n14;

SELECT from_unixtime(1226446340), to_date(from_unixtime(1226446340)), day('2008-11-01'), month('2008-11-01'), year('2008-11-01'), day('2008-11-01 15:32:20'), month('2008-11-01 15:32:20'), year('2008-11-01 15:32:20') FROM dest1_n14;

EXPLAIN
SELECT from_unixtime(unix_timestamp('2010-01-13 11:57:40', 'yyyy-MM-dd HH:mm:ss'), 'MM/dd/yy HH:mm:ss'), from_unixtime(unix_timestamp('2010-01-13 11:57:40')) from dest1_n14;

SELECT from_unixtime(unix_timestamp('2010-01-13 11:57:40', 'yyyy-MM-dd HH:mm:ss'), 'MM/dd/yy HH:mm:ss'), from_unixtime(unix_timestamp('2010-01-13 11:57:40')) from dest1_n14;

EXPLAIN
SELECT from_unixtime(unix_timestamp('2010-01-13 11:57:40', 'yyyy-MM-dd HH:mm:ss'), 'MM/dd/uu HH:mm:ss'), from_unixtime(unix_timestamp('2010-01-13 11:57:40')) from dest1_n14;

SELECT from_unixtime(unix_timestamp('2010-01-13 11:57:40', 'yyyy-MM-dd HH:mm:ss'), 'MM/dd/uu HH:mm:ss'), from_unixtime(unix_timestamp('2010-01-13 11:57:40')) from dest1_n14;

SELECT from_unixtime(unix_timestamp(cast('2010-01-13' as date)));

SELECT from_unixtime(unix_timestamp('2010-01-13 11:57:40', 'yyyy-MM-dd HH:mm:ss'), 'MM/dd/yy HH:mm:ss');

SELECT from_unixtime(unix_timestamp('2010-01-13 11:57:40', 'yyyy-MM-dd HH:mm:ss'), 'MM/dd/uu HH:mm:ss');

select from_unixtime(unix_timestamp('2010-01-13 11:57:40'), 'EEEE');

select from_unixtime(unix_timestamp(cast('2010-01-13 11:57:40' || ' America/Los_Angeles' as timestamp with local time zone)) ,"yyyy-MM-dd'T'HH:mm:ssXXX") ;

select from_unixtime(unix_timestamp(cast('2010-01-13 11:57:40' || ' America/Los_Angeles' as timestamp with local time zone)) ,"uuuu-MM-dd'T'HH:mm:ssXXX") ;

select from_unixtime(to_unix_timestamp(cast('2021-01-01' as date)));

select from_unixtime(to_unix_timestamp(cast('1400-01-01' as date)));

select from_unixtime(to_unix_timestamp(cast('1800-01-01' as date)));

select from_unixtime(to_unix_timestamp(cast('1900-01-01' as date)));

select from_unixtime(to_unix_timestamp(cast('2000-01-07' as date)));

select from_unixtime(to_unix_timestamp(cast('0000-00-00' as date)));

set time zone Europe/Rome;

SELECT from_unixtime(1226446340), to_date(from_unixtime(1226446340)), day('2008-11-01'), month('2008-11-01'), year('2008-11-01'), day('2008-11-01 15:32:20'), month('2008-11-01 15:32:20'), year('2008-11-01 15:32:20') FROM dest1_n14;

set hive.local.time.zone=Asia/Bangkok;

select from_unixtime(unix_timestamp('1400-11-08 01:53:11'));
select from_unixtime(unix_timestamp('1800-11-08 01:53:11'));
select from_unixtime(unix_timestamp('1400-11-08 08:00:00 ICT', 'yyyy-MM-dd HH:mm:ss z'));
select from_unixtime(unix_timestamp('1800-11-08 08:00:00 ICT', 'yyyy-MM-dd HH:mm:ss z'));
select from_unixtime(unix_timestamp('0000-00-00', 'uuuu-MM-dd'));
select from_unixtime(unix_timestamp("2001.07.04 AD at 12:08:56 ICT","yyyy.MM.dd G 'at' HH:mm:ss z"));

set hive.local.time.zone=Europe/London;

select from_unixtime(unix_timestamp('1400-11-08 01:53:11'));
select from_unixtime(unix_timestamp('1400-11-08', 'yyyy-MM-dd'));
select from_unixtime(unix_timestamp("Wed, Jul 4, '01", "EEE, MMM d, ''yy"));

set hive.local.time.zone=US/Hawaii;

select from_unixtime(unix_timestamp('1400-11-08 01:53:11'));
select from_unixtime(unix_timestamp('1800-11-08 01:53:11'));
select from_unixtime(to_unix_timestamp('0000-00-00'));
