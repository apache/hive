DESCRIBE FUNCTION last_day;
DESCRIBE FUNCTION EXTENDED last_day;

explain select last_day('2015-02-05');

select
last_day('2014-01-01'),
last_day('2014-01-14'),
last_day('2014-01-31'),
last_day('2014-02-02'),
last_day('2014-02-28'),
last_day('2016-02-03'),
last_day('2016-02-28'),
last_day('2016-02-29'),
last_day(cast(null as string));


select
last_day('2014-01-01 10:30:45'),
last_day('2014-01-14 10:30:45'),
last_day('2014-01-31 10:30:45'),
last_day('2014-02-02 10:30:45'),
last_day('2014-02-28 10:30:45'),
last_day('2016-02-03 10:30:45'),
last_day('2016-02-28 10:30:45'),
last_day('2016-02-29 10:30:45'),
last_day(cast(null as string));


select
last_day(cast('2014-01-01' as date)),
last_day(cast('2014-01-14' as date)),
last_day(cast('2014-01-31' as date)),
last_day(cast('2014-02-02' as date)),
last_day(cast('2014-02-28' as date)),
last_day(cast('2016-02-03' as date)),
last_day(cast('2016-02-28' as date)),
last_day(cast('2016-02-29' as date)),
last_day(cast(null as date));

select
last_day(cast('2014-01-01 10:30:45' as timestamp)),
last_day(cast('2014-01-14 10:30:45' as timestamp)),
last_day(cast('2014-01-31 10:30:45' as timestamp)),
last_day(cast('2014-02-02 10:30:45' as timestamp)),
last_day(cast('2014-02-28 10:30:45' as timestamp)),
last_day(cast('2016-02-03 10:30:45' as timestamp)),
last_day(cast('2016-02-28 10:30:45' as timestamp)),
last_day(cast('2016-02-29 10:30:45' as timestamp)),
last_day(cast(null as timestamp));
