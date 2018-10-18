-- Setup
create table t1 (c1 int);

-- Check value of parameter
set hive.metastore.disallow.incompatible.col.type.changes;
set metaconf:hive.metastore.disallow.incompatible.col.type.changes;

-- Change parameter to disallow column type changes in metastore
set hive.metastore.disallow.incompatible.col.type.changes=false;
set metaconf:hive.metastore.disallow.incompatible.col.type.changes=true;

-- check value of parameter
set hive.metastore.disallow.incompatible.col.type.changes;
set metaconf:hive.metastore.disallow.incompatible.col.type.changes;

-- Change int to small int now not allowed.
alter table t1 change column c1 c1 smallint;
