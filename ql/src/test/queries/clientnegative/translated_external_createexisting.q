set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;
set metastore.metadata.transformer.location.mode=prohibit;

set hive.fetch.task.conversion=none;
set hive.compute.query.using.stats=false;

create table t (a integer);

-- table should be translated
desc formatted t;

create table t (a integer);

