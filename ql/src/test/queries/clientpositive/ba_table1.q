--! qt:dataset:src
-- SORT_QUERY_RESULTS

drop table ba_test_n4;

-- This query tests a) binary type works correctly in grammar b) string can be cast into binary c) binary can be stored in a table d) binary data can be loaded back again and queried d) order-by on a binary key 

create table ba_test_n4 (ba_key binary, ba_val binary) ;

describe extended ba_test_n4;

from src insert overwrite table ba_test_n4 select cast (src.key as binary), cast (src.value as binary);

select * from ba_test_n4 tablesample (10 rows);

drop table ba_test_n4;
