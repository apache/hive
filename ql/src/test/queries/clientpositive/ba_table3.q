--! qt:dataset:src
drop table ba_test_n2;

-- All the tests of ba_table1.q + test for a group-by and aggregation on a binary key.

create table ba_test_n2 (ba_key binary, ba_val binary) ;

from src insert overwrite table ba_test_n2 select cast (src.key as binary), cast (src.value as binary);

select ba_test_n2.ba_key, count(ba_test_n2.ba_val) from ba_test_n2 group by ba_test_n2.ba_key order by ba_key limit 5;

drop table ba_test_n2;


