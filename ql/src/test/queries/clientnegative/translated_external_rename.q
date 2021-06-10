set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;
set metastore.metadata.transformer.location.mode=prohibit;

set hive.fetch.task.conversion=none;
set hive.compute.query.using.stats=false;

create external table t (a integer);
insert into t values(1);
alter table t rename to t2;
create table t (a integer);

