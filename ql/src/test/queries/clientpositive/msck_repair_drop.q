set hive.mv.files.thread=0;
DROP TABLE IF EXISTS repairtable_n1;

CREATE TABLE repairtable_n1(col STRING) PARTITIONED BY (p1 STRING, p2 STRING);

-- repairtable_n1 will have partitions created with part keys p1=1, p1=2, p1=3, p1=4 and p1=5
-- p1=2 will be used to test drop in 3 tests
--   1.  each partition is dropped individually: set hive.msck.repair.batch.size=1;
--   2.  partition are dropped in groups of 3:  set hive.msck.repair.batch.size=3;
--   3.  all partitions are dropped in 1 shot:  set hive.msck.repair.batch.size=0;
-- the same set of 10 partitions will be created between each drop attempts
-- p1=3, p1=4 and p1=5 will be used to test keywords add, drop and sync

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=1/p2=11/p3=111;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=1/p2=11/p3=111/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=1/p2=12/p3=121;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=1/p2=12/p3=121/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=21/p3=211;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=21/p3=211/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=22/p3=221;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=22/p3=221/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=23/p3=231;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=23/p3=231/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=24/p3=241;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=24/p3=241/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=25/p3=251;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=25/p3=251/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=26/p3=261;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=26/p3=261/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=27/p3=271;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=27/p3=271/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=28/p3=281;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=28/p3=281/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=29/p3=291;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=29/p3=291/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=210/p3=2101;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=210/p3=2101/datafile;

MSCK TABLE default.repairtable_n1;
MSCK REPAIR TABLE default.repairtable_n1;

-- Now all 12 partitions are in
show partitions default.repairtable_n1;

-- Remove all p1=2 partitions from file system
dfs -rmr ${system:test.warehouse.dir}/repairtable_n1/p1=2;

-- test 1: each partition is dropped individually
set hive.msck.repair.batch.size=1;
MSCK TABLE default.repairtable_n1 DROP PARTITIONS;
MSCK REPAIR TABLE default.repairtable_n1 DROP PARTITIONS;
show partitions default.repairtable_n1;

-- Recreate p1=2 partitions
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=21/p3=211;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=21/p3=211/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=22/p3=221;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=22/p3=221/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=23/p3=231;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=23/p3=231/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=24/p3=241;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=24/p3=241/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=25/p3=251;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=25/p3=251/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=26/p3=261;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=26/p3=261/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=27/p3=271;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=27/p3=271/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=28/p3=281;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=28/p3=281/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=29/p3=291;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=29/p3=291/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=210/p3=2101;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=210/p3=2101/datafile;

MSCK TABLE default.repairtable_n1;
MSCK REPAIR TABLE default.repairtable_n1;

-- Now all 12 partitions are in
show partitions default.repairtable_n1;

-- Remove all p1=2 partitions from file system
dfs -rmr ${system:test.warehouse.dir}/repairtable_n1/p1=2;

-- test 2: partition are dropped in groups of 3
set hive.msck.repair.batch.size=3;
MSCK TABLE default.repairtable_n1 DROP PARTITIONS;
MSCK REPAIR TABLE default.repairtable_n1 DROP PARTITIONS;
show partitions default.repairtable_n1;

-- Recreate p1=2 partitions
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=21/p3=211;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=21/p3=211/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=22/p3=221;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=22/p3=221/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=23/p3=231;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=23/p3=231/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=24/p3=241;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=24/p3=241/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=25/p3=251;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=25/p3=251/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=26/p3=261;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=26/p3=261/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=27/p3=271;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=27/p3=271/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=28/p3=281;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=28/p3=281/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=29/p3=291;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=29/p3=291/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=210/p3=2101;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=2/p2=210/p3=2101/datafile;

MSCK TABLE default.repairtable_n1;
MSCK REPAIR TABLE default.repairtable_n1;

-- Now all 12 partitions are in
show partitions default.repairtable_n1;

-- Remove all p1=2 partitions from file system
dfs -rmr ${system:test.warehouse.dir}/repairtable_n1/p1=2;

--  test 3.  all partitions are dropped in 1 shot
set hive.msck.repair.batch.size=0;
MSCK TABLE default.repairtable_n1 DROP PARTITIONS;
MSCK REPAIR TABLE default.repairtable_n1 DROP PARTITIONS;
show partitions default.repairtable_n1;

-- test add parition keyword: begin
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=3/p2=31/p3=311;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=3/p2=31/p3=311/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=3/p2=32/p3=321;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=3/p2=32/p3=321/datafile;

MSCK TABLE default.repairtable_n1;
MSCK REPAIR TABLE default.repairtable_n1;
show partitions default.repairtable_n1;

-- Create p1=4 in filesystem
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=4/p2=41/p3=411;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=4/p2=41/p3=411/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=4/p2=42/p3=421;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=4/p2=42/p3=421/datafile;

-- Remove p1=3 from filesystem
dfs -rmr ${system:test.warehouse.dir}/repairtable_n1/p1=3;

-- Status: p1=3 dropped from filesystem, but exists in metastore
--         p1=4 exists in filesystem but not in metastore
-- test add partition: only brings in p1=4 and doesn't remove p1=3
MSCK TABLE default.repairtable_n1 ADD PARTITIONS;
MSCK REPAIR TABLE default.repairtable_n1 ADD PARTITIONS;
show partitions default.repairtable_n1;
-- test add partition keyword: end

-- test drop partition keyword: begin
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=5/p2=51/p3=511;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=5/p2=51/p3=511/datafile;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n1/p1=5/p2=52/p3=521;
dfs -touchz ${system:test.warehouse.dir}/repairtable_n1/p1=5/p2=52/p3=521/datafile;

-- Status: p1=3 removed from filesystem, but exists in metastore (as part of add test)
--         p1=5 exists in filesystem but not in metastore
-- test drop partition: only removes p1=3 from metastore but doesn't update metadata for p1=5
MSCK TABLE default.repairtable_n1 DROP PARTITIONS;
MSCK REPAIR TABLE default.repairtable_n1 DROP PARTITIONS;
show partitions default.repairtable_n1;
-- test drop partition keyword: end

-- test sync partition keyword: begin
-- Remove p1=4 from filesystem
dfs -rmr ${system:test.warehouse.dir}/repairtable_n1/p1=4;

-- Status: p1=4 dropped from filesystem, but exists in metastore
--         p1=5 exists in filesystem but not in metastore (as part of drop test)
-- test sync partition: removes p1=4 from metastore and updates metadata for p1=5
MSCK TABLE default.repairtable_n1 SYNC PARTITIONS;
MSCK REPAIR TABLE default.repairtable_n1 SYNC PARTITIONS;
show partitions default.repairtable_n1;
-- test sync partition keyword: end
