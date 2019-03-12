declare v_date date;
declare v_timestamp timestamp(17, 3);

select
  cast('2019-02-20 12:23:45.678' as date),
  cast('2019-02-20 12:23:45.678' as timestamp)
into
  v_date,
  v_timestamp
from src limit 1;
        
print 'date: ' || v_date;
print 'timestamp: ' || v_timestamp;
