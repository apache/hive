set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION next_day;
DESCRIBE FUNCTION EXTENDED next_day;

EXPLAIN SELECT next_day('2014-01-14', 'MO')
FROM src tablesample (1 rows);

SELECT next_day('2015-01-11', 'su'),
       next_day('2015-01-11', 'MO'),
       next_day('2015-01-11', 'Tu'),
       next_day('2015-01-11', 'wE'),
       next_day('2015-01-11', 'th'),
       next_day('2015-01-11', 'FR'),
       next_day('2015-01-11', 'Sa')
FROM src tablesample (1 rows);

SELECT next_day('2015-01-17 00:02:30', 'sun'),
       next_day('2015-01-17 00:02:30', 'MON'),
       next_day('2015-01-17 00:02:30', 'Tue'),
       next_day('2015-01-17 00:02:30', 'weD'),
       next_day('2015-01-17 00:02:30', 'tHu'),
       next_day('2015-01-17 00:02:30', 'FrI'),
       next_day('2015-01-17 00:02:30', 'SAt')
FROM src tablesample (1 rows);

SELECT next_day(cast('2015-01-14 14:04:34' as timestamp), 'sunday'),
       next_day(cast('2015-01-14 14:04:34' as timestamp), 'Monday'),
       next_day(cast('2015-01-14 14:04:34' as timestamp), 'Tuesday'),
       next_day(cast('2015-01-14 14:04:34' as timestamp), 'wednesday'),
       next_day(cast('2015-01-14 14:04:34' as timestamp), 'thursDAY'),
       next_day(cast('2015-01-14 14:04:34' as timestamp), 'FRIDAY'),
       next_day(cast('2015-01-14 14:04:34' as timestamp), 'SATurday')
FROM src tablesample (1 rows);

SELECT next_day(cast(null as string), 'MO'),
       next_day(cast(null as timestamp), 'MO'),
       next_day('2015-01-11', cast(null as string)),
       next_day(cast(null as string), cast(null as string)),
       next_day(cast(null as timestamp), cast(null as string))
FROM src tablesample (1 rows);

SELECT next_day('2015-02-02', 'VT'),
       next_day('2015-02-30', 'WE'),
       next_day('02/15/2015', 'WE')
FROM src tablesample (1 rows);
