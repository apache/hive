set hive.default.fileformat=ORC;
create table orc_staging (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp);
create table orc_test (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp) partitioned by (ds string);

load data local inpath '../../data/files/orc_split_elim.orc' into table orc_staging;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_staging/;

load data inpath '${hiveconf:hive.metastore.warehouse.dir}/orc_staging/orc_split_elim.orc' into table orc_test partition (ds='10');
load data local inpath '../../data/files/orc_split_elim.orc' into table orc_test partition (ds='10');
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_test/ds=10/;

load data local inpath '../../data/files/orc_split_elim.orc' overwrite into table orc_staging;
load data inpath '${hiveconf:hive.metastore.warehouse.dir}/orc_staging/' overwrite into table orc_test partition (ds='10');
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_test/ds=10/;

alter table orc_test add partition(ds='11');
alter table orc_test partition(ds='11') set fileformat textfile;
load data local inpath '../../data/files/kv1.txt' into table orc_test partition(ds='11');
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_test/ds=11/;

alter table orc_test add partition(ds='12');
alter table orc_test partition(ds='12') set fileformat textfile;
load data local inpath '../../data/files/types/primitives' into table orc_test partition(ds='12');
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_test/ds=12/;
