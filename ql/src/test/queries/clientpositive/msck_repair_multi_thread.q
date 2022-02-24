set hive.metastore.fshandler.threads=2;
set hive.metastore.msck.fshandler.threads=2;

DROP TABLE IF EXISTS repairtable_hive_25980;

CREATE EXTERNAL TABLE repairtable_hive_25980(col STRING) PARTITIONED BY (p1 STRING, p2 STRING) LOCATION '${system:test.warehouse.dir}/repairtable_hive_25980';

MSCK REPAIR TABLE repairtable_hive_25980;

show partitions repairtable_hive_25980;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_hive_25980/p1=a/p2=aa;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=ab;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=ac;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=ad;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=ae;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=af;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=ag;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=ah;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=ai;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=aj;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=ak;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=al;

MSCK REPAIR TABLE repairtable_hive_25980;

show partitions repairtable_hive_25980;

dfs -rmdir ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=ai;
dfs -rmdir ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=aj;
dfs -rmdir ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=ak;
dfs -rmdir ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=al;

MSCK REPAIR TABLE default.repairtable_hive_25980 DROP PARTITIONS;

show partitions default.repairtable_hive_25980;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=ai;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=aj;
dfs -rmdir ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=ag;
dfs -rmdir ${system:test.warehouse.dir}/repairtable_hive_25980/p1=b/p2=ah;

set hive.metastore.fshandler.threads=30;
set hive.metastore.msck.fshandler.threads=30;

MSCK REPAIR TABLE repairtable_hive_25980 SYNC PARTITIONS;

show partitions repairtable_hive_25980;

DROP TABLE default.repairtable_hive_25980;
