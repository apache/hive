-- valid date value
select cast ('1945-12-31' as date);
select cast ('1946-01-01' as date);
select cast ('0004-05-06' as date);
select cast ('2001-11-12 01:02:03' as date);
select cast (' 2001-11-12 01:02:03' as date);
select cast (' 1946-01-01' as date);

--date
-- invalid date value
select cast ('2001-01-32' as date);
select cast ('2001-13-30' as date);
select cast ('0000-00-00' as date);

--valid timestamp value
select cast ('1945-12-31 23:59:59.0"' as timestamp);
select cast ('1945-12-31 23:59:59.1234' as timestamp);
select cast ('1970-01-01 00:00:00' as timestamp);
--timestamp
--invalid timestamp value
select cast ('0000-00-00 00:00:00' as timestamp);
