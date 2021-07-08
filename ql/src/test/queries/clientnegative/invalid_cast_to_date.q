--date
-- invalid date value
select cast ('2001-01-32' as date);
select cast ('2001-13-30' as date);
select cast ('0000-00-00' as date);
