explain select cast('2005-01-03 02:01:00 GMT' as timestamp with local time zone);
select cast('2005-01-03 02:01:00 GMT' as timestamp with local time zone);

explain select cast('2016-01-03 12:26:34.0123 America/Los_Angeles' as timestamplocaltz);
select cast('2016-01-03 12:26:34.0123 America/Los_Angeles' as timestamplocaltz);

explain select cast('2016-01-03Europe/London' as timestamplocaltz);
select cast('2016-01-03Europe/London' as timestamplocaltz);

explain select cast('2016-01-03 13:34:56.38 +1:00' as timestamplocaltz);
select cast('2016-01-03 13:34:56.38 +1:00' as timestamplocaltz);
