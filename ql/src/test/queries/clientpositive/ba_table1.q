-- SORT_QUERY_RESULTS

drop table ba_test;

-- This query tests a) binary type works correctly in grammar b) string can be cast into binary c) binary can be stored in a table d) binary data can be loaded back again and queried d) order-by on a binary key 

create table ba_test (ba_key binary, ba_val binary) ;

describe extended ba_test;

from src insert overwrite table ba_test select cast (src.key as binary), cast (src.value as binary);

select * from ba_test tablesample (10 rows);

drop table ba_test;
