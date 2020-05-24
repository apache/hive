--! qt:dataset:src
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

explain select cast('2011-01-01 01:01:01' as timestamp) as c from src union select cast('2011-01-01 01:01:01' as timestamp) as c from src limit 5;

explain extended select cast('2011-01-01 01:01:01' as timestamp) as c from src union select cast('2011-01-01 01:01:01' as timestamp) as c from src limit 5;

select cast('2011-01-01 01:01:01' as timestamp) as c from src union select cast('2011-01-01 01:01:01' as timestamp) as c from src limit 5;

explain select cast('2011-01-01 01:01:01.123' as timestamp) as c from src union select cast('2011-01-01 01:01:01.123' as timestamp) as c from src limit 5;

select cast('2011-01-01 01:01:01.123' as timestamp) as c from src union select cast('2011-01-01 01:01:01.123' as timestamp) as c from src limit 5;
