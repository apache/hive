DROP TABLE IF EXISTS repairtable_hive_25980;

CREATE TABLE repairtable_hive_25980(id int, name string) partitioned by(year int,month int);

MSCK REPAIR TABLE repairtable_hive_25980;

SHOW PARTITIONS repairtable_hive_25980;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_hive_25980/year=2022/month=01;
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_hive_25980/year=2022/month=03;
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_hive_25980/year=2022/month=04;
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_hive_25980/year=2021/month=02;
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_hive_25980/year=2021/month=01;
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_hive_25980/year=2021/month=03;

MSCK REPAIR TABLE repairtable_hive_25980;

SHOW PARTITIONS repairtable_hive_25980;

dfs -rmdir ${system:test.local.warehouse.dir}/repairtable_hive_25980/year=2021/month=02;
dfs -rmdir ${system:test.local.warehouse.dir}/repairtable_hive_25980/year=2021/month=01;
dfs -rmdir ${system:test.local.warehouse.dir}/repairtable_hive_25980/year=2021/month=03;
dfs -rmdir ${system:test.local.warehouse.dir}/repairtable_hive_25980/year=2021;
dfs ${system:test.dfs.mkdir} file:///tmp/repairtable_hive_25980_external_dir/year=2022/month=02;
dfs ${system:test.dfs.mkdir} file:///tmp/repairtable_hive_25980_external_dir/year=2021/month=04;
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_hive_25980/year=2022/month=12;

alter table repairtable_hive_25980 add partition(year=2022,month=02) location 'file:///tmp/repairtable_hive_25980_external_dir/year=2022/month=02';
alter table repairtable_hive_25980 add partition(year=2021,month=04) location 'file:///tmp/repairtable_hive_25980_external_dir/year=2021/month=04';

MSCK REPAIR TABLE repairtable_hive_25980 SYNC PARTITIONS;

SHOW PARTITIONS repairtable_hive_25980;
