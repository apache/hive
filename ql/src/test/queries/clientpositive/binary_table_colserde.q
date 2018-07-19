--! qt:dataset:src
drop table ba_test_n0;

-- Everything in ba_table1.q + columnar serde in RCFILE.

create table ba_test_n0 (ba_key binary, ba_val binary) stored as rcfile;
alter table ba_test_n0 set serde 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';

describe extended ba_test_n0;

from src insert overwrite table ba_test_n0 select cast (src.key as binary), cast (src.value as binary);

select ba_key, ba_val from ba_test_n0 order by ba_key limit 10;

drop table ba_test_n0;


