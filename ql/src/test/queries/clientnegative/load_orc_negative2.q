create table text_test (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp);
load data local inpath '../../data/files/kv1.txt' into table text_test;

set hive.default.fileformat=ORC;
create table orc_test (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp);
load data inpath '${hiveconf:hive.metastore.warehouse.dir}/text_test/kv1.txt' into table orc_test;
