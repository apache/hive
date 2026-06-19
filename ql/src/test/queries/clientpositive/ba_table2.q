--! qt:dataset:src
-- SORT_QUERY_RESULTS

drop table ba_test_n3;

-- All the test in ba_test1.q + using LazyBinarySerde instead of LazySimpleSerde

create table ba_test_n3 (ba_key binary, ba_val binary) ;
alter table ba_test_n3 set serde 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe';

describe extended ba_test_n3;

from src insert overwrite table ba_test_n3 select cast (src.key as binary), cast (src.value as binary);

select * from ba_test_n3 tablesample (10 rows);

drop table ba_test_n3;


