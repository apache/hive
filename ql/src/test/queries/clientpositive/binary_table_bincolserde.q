--! qt:dataset:src
drop table ba_test_n1;

-- Tests everything in binary_table_colserde.q + uses LazyBinaryColumnarSerde

create table ba_test_n1 (ba_key binary, ba_val binary) stored as rcfile;
alter table ba_test_n1 set serde 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe';

describe extended ba_test_n1;

from src insert overwrite table ba_test_n1 select cast (src.key as binary), cast (src.value as binary);

select ba_key, ba_val from ba_test_n1 order by ba_key limit 10;

drop table ba_test_n1;


