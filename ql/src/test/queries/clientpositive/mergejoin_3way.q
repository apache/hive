--! qt:dataset:src1
--! qt:dataset:src
set hive.auto.convert.join=false;
set hive.cbo.enable=false;

select
  a.key, b.value, c.value
from
  src a,
  src1 b,
  src1 c
where
  a.key = b.key and a.key = c.key
  and b.key != '' and b.value != ''
  and a.value > 'wal_6789'
  and c.value > 'wal_6789'
;
