-- Setup
create table t1_n14 (c1 int);

-- Check value of parameter
set hive.metastore.disallow.incompatible.col.type.changes;
set metaconf:hive.metastore.disallow.incompatible.col.type.changes;

-- Change parameter to allow column type changes in metastore
set metaconf:hive.metastore.disallow.incompatible.col.type.changes=false;

-- check value of parameter
set hive.metastore.disallow.incompatible.col.type.changes;
set metaconf:hive.metastore.disallow.incompatible.col.type.changes;

-- Change int to small int now allowed.
alter table t1_n14 change column c1 c1 smallint;
