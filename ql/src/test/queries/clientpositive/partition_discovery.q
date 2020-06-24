set hive.msck.repair.batch.size=1;
set hive.mv.files.thread=0;

DROP TABLE IF EXISTS repairtable_n7;
DROP TABLE IF EXISTS repairtable_n8;
DROP TABLE IF EXISTS repairtable_n9;
DROP TABLE IF EXISTS repairtable_n10;

CREATE EXTERNAL TABLE repairtable_n7(col STRING) PARTITIONED BY (p1 STRING, p2 STRING)
LOCATION '${system:test.warehouse.dir}/repairtable_n7';

describe formatted repairtable_n7;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n7/p1=a/p2=b/;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n7/p1=c/p2=d/;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n7/p1=a/p2=b/datafile;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n7/p1=c/p2=d/datafile;

MSCK REPAIR TABLE default.repairtable_n7;
show partitions default.repairtable_n7;

CREATE EXTERNAL TABLE repairtable_n8 LIKE repairtable_n7
LOCATION '${system:test.warehouse.dir}/repairtable_n8';

describe formatted repairtable_n8;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n8/p1=a/p2=b/;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n8/p1=c/p2=d/;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n8/p1=a/p2=b/datafile;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n8/p1=c/p2=d/datafile;

MSCK REPAIR TABLE default.repairtable_n8;
show partitions default.repairtable_n8;

CREATE EXTERNAL TABLE repairtable_n9(col STRING) PARTITIONED BY (p1 STRING, p2 STRING)
LOCATION '${system:test.warehouse.dir}/repairtable_n9' tblproperties ("partition.retention.period"="10s");

describe formatted repairtable_n9;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n9/p1=a/p2=b/;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n9/p1=c/p2=d/;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n9/p1=a/p2=b/datafile;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n9/p1=c/p2=d/datafile;

set metastore.msck.repair.enable.partition.retention=false;
MSCK REPAIR TABLE default.repairtable_n9;
show partitions default.repairtable_n9;

!sleep 12;

set metastore.msck.repair.enable.partition.retention=true;
-- msck does not drop partitions, so this still should be no-op
MSCK REPAIR TABLE default.repairtable_n9;
show partitions default.repairtable_n9;

-- this will drop old partitions
MSCK REPAIR TABLE default.repairtable_n9 SYNC PARTITIONS;
show partitions default.repairtable_n9;

CREATE EXTERNAL TABLE repairtable_n10 PARTITIONED BY(p1,p2) STORED AS ORC AS SELECT * FROM repairtable_n9;
describe formatted repairtable_n10;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_n10/p1=a/p2=b/;
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_n10/p1=c/p2=d/;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_n10/p1=a/p2=b/datafile;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_n10/p1=c/p2=d/datafile;

set metastore.msck.repair.enable.partition.retention=false;
!sleep 12;
MSCK REPAIR TABLE default.repairtable_n10;
show partitions default.repairtable_n10;


CREATE EXTERNAL TABLE repairtable_n11 LIKE repairtable_n10;
describe formatted repairtable_n11;

ALTER TABLE repairtable_n10 SET TBLPROPERTIES('discover.partitions'='false');
describe formatted repairtable_n10;

-- tbl params are not retained by default
CREATE EXTERNAL TABLE repairtable_n12 LIKE repairtable_n10;
describe formatted repairtable_n12;

set hive.ddl.createtablelike.properties.whitelist=discover.partitions;
-- with tbl params retainer
CREATE EXTERNAL TABLE repairtable_n13 LIKE repairtable_n10;
describe formatted repairtable_n13;


DROP TABLE default.repairtable_n7;
DROP TABLE default.repairtable_n8;
DROP TABLE default.repairtable_n9;
DROP TABLE default.repairtable_n10;
