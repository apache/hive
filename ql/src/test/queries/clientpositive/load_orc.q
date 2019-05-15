set hive.default.fileformat=ORC;
create table orc_staging_n0 (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp);
create table orc_test_n1 (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp);

load data local inpath '../../data/files/orc_split_elim.orc' into table orc_staging_n0;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_staging_n0/;

load data inpath '${hiveconf:hive.metastore.warehouse.dir}/orc_staging_n0/orc_split_elim.orc' into table orc_test_n1;
load data local inpath '../../data/files/orc_split_elim.orc' into table orc_test_n1;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_test_n1/;
