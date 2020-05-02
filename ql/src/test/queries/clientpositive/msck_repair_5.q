DROP TABLE IF EXISTS repairtable_n5;

CREATE TABLE repairtable_n5(key int) partitioned by (Year int, Month int, Value string);

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n5/Year=2020/Month=03/Value=Val1;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n5/Year=2020/Month=03/Value=Val1/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n5/Year=2020/Month=03/Value=Val2;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n5/Year=2020/Month=03/Value=Val2/datafile;

MSCK REPAIR TABLE default.repairtable_n5;
SHOW PARTITIONS default.repairtable_n5;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n5/Year=2020/Month=03/Value=val3;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n5/Year=2020/Month=03/Value=val3/datafile;

MSCK REPAIR TABLE default.repairtable_n5;
SHOW PARTITIONS default.repairtable_n5;

DROP TABLE default.repairtable_n5;
