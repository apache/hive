DESCRIBE FUNCTION to_utc_timestamp;
DESC FUNCTION EXTENDED to_utc_timestamp;

explain select to_utc_timestamp('2012-02-11 10:30:00', 'PST');

select
to_utc_timestamp('2012-02-10 20:30:00', 'PST'),
to_utc_timestamp('2012-02-11 08:30:00', 'Europe/Moscow'),
to_utc_timestamp('2012-02-11 12:30:00', 'GMT+8'),
to_utc_timestamp('2012-02-11 04:30:00', 'GMT'),
to_utc_timestamp(cast(null as string), 'PST'),
to_utc_timestamp('2012-02-11 04:30:00', cast(null as string));

select
to_utc_timestamp(cast('2012-02-10 20:30:00' as timestamp), 'PST'),
to_utc_timestamp(cast('2012-02-11 08:30:00' as timestamp), 'Europe/Moscow'),
to_utc_timestamp(cast('2012-02-11 12:30:00' as timestamp), 'GMT+8'),
to_utc_timestamp(cast('2012-02-11 04:30:00' as timestamp), 'GMT'),
to_utc_timestamp(cast(null as timestamp), 'PST'),
to_utc_timestamp(cast('2012-02-11 04:30:00' as timestamp), cast(null as string));
