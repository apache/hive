--! qt:dataset:src
CREATE TABLE dest1_n14(c1 STRING) STORED AS TEXTFILE;

FROM src INSERT OVERWRITE TABLE dest1_n14 SELECT '  abc  ' WHERE src.key = 86;

EXPLAIN
SELECT from_unixtime(1226446340), to_date(from_unixtime(1226446340)), day('2008-11-01'), month('2008-11-01'), year('2008-11-01'), day('2008-11-01 15:32:20'), month('2008-11-01 15:32:20'), year('2008-11-01 15:32:20') FROM dest1_n14;

SELECT from_unixtime(1226446340), to_date(from_unixtime(1226446340)), day('2008-11-01'), month('2008-11-01'), year('2008-11-01'), day('2008-11-01 15:32:20'), month('2008-11-01 15:32:20'), year('2008-11-01 15:32:20') FROM dest1_n14;

EXPLAIN
SELECT from_unixtime(unix_timestamp('2010-01-13 11:57:40', 'yyyy-MM-dd HH:mm:ss'), 'MM/dd/yy HH:mm:ss'), from_unixtime(unix_timestamp('2010-01-13 11:57:40')) from dest1_n14;

SELECT from_unixtime(unix_timestamp('2010-01-13 11:57:40', 'yyyy-MM-dd HH:mm:ss'), 'MM/dd/yy HH:mm:ss'), from_unixtime(unix_timestamp('2010-01-13 11:57:40')) from dest1_n14;

set time zone Europe/Rome;

SELECT from_unixtime(1226446340), to_date(from_unixtime(1226446340)), day('2008-11-01'), month('2008-11-01'), year('2008-11-01'), day('2008-11-01 15:32:20'), month('2008-11-01 15:32:20'), year('2008-11-01 15:32:20') FROM dest1_n14;
