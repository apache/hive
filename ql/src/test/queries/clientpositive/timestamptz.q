explain select cast('2005-01-03 02:01:00 GMT' as timestamp with time zone);
select cast('2005-01-03 02:01:00 GMT' as timestamp with time zone);

explain select cast('2016-01-03 12:26:34.0123 America/Los_Angeles' as timestamptz);
select cast('2016-01-03 12:26:34.0123 America/Los_Angeles' as timestamptz);

explain select cast('2016-01-03Europe/London' as timestamptz);
select cast('2016-01-03Europe/London' as timestamptz);

explain select cast('2016-01-03 13:34:56.38 +1:00' as timestamptz);
select cast('2016-01-03 13:34:56.38 +1:00' as timestamptz);
