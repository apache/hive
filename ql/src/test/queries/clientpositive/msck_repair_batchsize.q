set hive.msck.repair.batch.size=2;

DROP TABLE IF EXISTS repairtable_n0;

CREATE TABLE repairtable_n0(col STRING) PARTITIONED BY (p1 STRING, p2 STRING);

MSCK TABLE repairtable_n0;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n0/p1=a/p2=a;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n0/p1=b/p2=a;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n0/p1=c/p2=a;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n0/p1=a/p2=a/datafile;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n0/p1=b/p2=a/datafile;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n0/p1=c/p2=a/datafile;

MSCK TABLE default.repairtable_n0;
show partitions default.repairtable_n0;

MSCK REPAIR TABLE default.repairtable_n0;
show partitions default.repairtable_n0;

MSCK TABLE repairtable_n0;
show partitions repairtable_n0;

DROP TABLE default.repairtable_n0;


dfs  ${system:test.dfs.mkdir} -p ${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n0/p1=c/p2=a/p3=b;
CREATE TABLE `repairtable_n0`( `col` string) PARTITIONED BY (  `p1` string,  `p2` string) location '${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n0/';

dfs -touchz ${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n0/p1=c/p2=a/p3=b/datafile;
set hive.mv.files.thread=1;
MSCK TABLE repairtable_n0;
show partitions repairtable_n0;

DROP TABLE default.repairtable_n0;
