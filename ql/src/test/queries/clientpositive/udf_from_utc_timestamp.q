DESCRIBE FUNCTION from_utc_timestamp;
DESC FUNCTION EXTENDED from_utc_timestamp;

explain select from_utc_timestamp('2012-02-11 10:30:00', 'PST');

select
from_utc_timestamp('2012-02-11 04:30:00', 'PST'),
from_utc_timestamp('2012-02-11 04:30:00', 'Europe/Moscow'),
from_utc_timestamp('2012-02-11 04:30:00', 'GMT+8'),
from_utc_timestamp('2012-02-11 04:30:00', 'GMT'),
from_utc_timestamp('2012-02-11 04:30:00', ''),
from_utc_timestamp('2012-02-11 04:30:00', '---'),
from_utc_timestamp(cast(null as string), 'PST'),
from_utc_timestamp('2012-02-11 04:30:00', cast(null as string));

select
from_utc_timestamp(cast('2012-02-11 04:30:00' as timestamp), 'PST'),
from_utc_timestamp(cast('2012-02-11 04:30:00' as timestamp), 'Europe/Moscow'),
from_utc_timestamp(cast('2012-02-11 04:30:00' as timestamp), 'GMT+8'),
from_utc_timestamp(cast('2012-02-11 04:30:00' as timestamp), 'GMT'),
from_utc_timestamp(cast('2012-02-11 04:30:00' as timestamp), ''),
from_utc_timestamp(cast('2012-02-11 04:30:00' as timestamp), '---'),
from_utc_timestamp(cast(null as timestamp), 'PST'),
from_utc_timestamp(cast('2012-02-11 04:30:00' as timestamp), cast(null as string));

select
from_utc_timestamp('2012-02-11-04:30:00', 'UTC'),
from_utc_timestamp('2012-02-11-04:30:00', 'PST');

select
to_epoch_milli(cast (1536449552291 as timestamp )),
to_epoch_milli(cast('2012-02-11 04:30:00' as timestamp)),
cast(to_epoch_milli(cast('2012-02-11 04:30:00' as timestamp)) as timestamp );