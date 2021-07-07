--date
-- invalid date value
select cast ('2001-01-32' as date);
select cast ('2001-13-30' as date);
select cast ('0000-00-00' as date);

--timestamp
--invalid timestamp value
select cast ('1945-12-45 23:59:59' as timestamp);
select cast ('1945-15-20 23:59:59' as timestamp);
select cast ('0000-00-00 00:00:00' as timestamp);