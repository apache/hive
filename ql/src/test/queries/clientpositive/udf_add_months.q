DESCRIBE FUNCTION add_months;
DESCRIBE FUNCTION EXTENDED add_months;

explain select add_months('2014-01-14', 1);

select
add_months('2014-01-14', 1),
add_months('2014-01-31', 1),
add_months('2014-02-28', -1),
add_months('2014-02-28', 2),
add_months('2014-04-30', -2),
add_months('2015-02-28', 12),
add_months('2016-02-29', -12),
add_months('2016-01-29', 1),
add_months('2016-02-29', -1),
add_months('2014-01-32', 1),
add_months('01/14/2014', 1),
add_months(cast(null as string), 1),
add_months('2014-01-14', cast(null as int));

select
add_months('2014-01-14 10:30:00', 1),
add_months('2014-01-31 10:30:00', 1),
add_months('2014-02-28 10:30:00', -1),
add_months('2014-02-28 16:30:00', 2),
add_months('2014-04-30 10:30:00', -2),
add_months('2015-02-28 10:30:00', 12),
add_months('2016-02-29 10:30:00', -12),
add_months('2016-01-29 10:30:00', 1),
add_months('2016-02-29 10:30:00', -1),
add_months('2014-01-32 10:30:00', 1);

select
add_months(cast('2014-01-14 10:30:00' as timestamp), 1),
add_months(cast('2014-01-31 10:30:00' as timestamp), 1),
add_months(cast('2014-02-28 10:30:00' as timestamp), -1),
add_months(cast('2014-02-28 16:30:00' as timestamp), 2),
add_months(cast('2014-04-30 10:30:00' as timestamp), -2),
add_months(cast('2015-02-28 10:30:00' as timestamp), 12),
add_months(cast('2016-02-29 10:30:00' as timestamp), -12),
add_months(cast('2016-01-29 10:30:00' as timestamp), 1),
add_months(cast('2016-02-29 10:30:00' as timestamp), -1),
add_months(cast(null as timestamp), 1);