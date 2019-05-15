
SELECT tumbling_window(cast ('2020-03-01 06:03:00' as timestamp), interval '5' MINUTES );
SELECT tumbling_window(cast ('2020-03-01 06:05:10' as timestamp), interval '5' HOUR);

SELECT tumbling_window(cast ('2020-03-01 00:04:59' as timestamp), interval '2' SECOND );
SELECT tumbling_window(cast ('2020-03-01 00:05:00' as timestamp), interval '2' SECOND );
SELECT tumbling_window(cast ('2020-03-01 00:05:01' as timestamp), interval '2' SECOND );
SELECT tumbling_window(cast ('2020-03-01 00:05:02' as timestamp), interval '2' SECOND );
SELECT tumbling_window(cast ('2020-03-01 00:05:03' as timestamp), interval '2' SECOND );

SELECT tumbling_window(cast ('2020-03-01 00:05:09' as timestamp), interval '3' SECOND ,cast ('2020-03-01 00:05:10' as timestamp));
SELECT tumbling_window(cast ('2020-03-01 00:05:10' as timestamp), interval '3' SECOND ,cast ('2020-03-01 00:05:10' as timestamp));
SELECT tumbling_window(cast ('2020-03-01 00:05:11' as timestamp), interval '3' SECOND ,cast ('2020-03-01 00:05:10' as timestamp));
SELECT tumbling_window(cast ('2020-03-01 00:05:12' as timestamp), interval '3' SECOND ,cast ('2020-03-01 00:05:10' as timestamp));
SELECT tumbling_window(cast ('2020-03-01 00:05:13' as timestamp), interval '3' SECOND ,cast ('2020-03-01 00:05:10' as timestamp));
SELECT tumbling_window(cast ('2020-03-01 00:05:14' as timestamp), interval '3' SECOND ,cast ('2020-03-01 00:05:10' as timestamp));

SELECT tumbling_window(cast ('2020-03-01 06:05:10' as timestamp), interval '5' HOUR, cast ('2020-03-01 01:05:10' as timestamp));
SELECT tumbling_window(cast ('2020-03-01 06:05:10' as timestamp), interval '5' HOUR, cast ('2020-03-01 02:05:10' as timestamp));