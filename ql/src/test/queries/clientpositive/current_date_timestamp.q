select current_timestamp = current_timestamp(), current_date = current_date() from src limit 5;

set hive.test.currenttimestamp =2012-01-01 01:02:03;
select current_date, current_timestamp from src limit 5;
