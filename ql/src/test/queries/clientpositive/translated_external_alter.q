set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;
set metastore.metadata.transformer.location.mode=seqsuffix;

set hive.fetch.task.conversion=none;
set hive.compute.query.using.stats=false;

create table caseSensitive (a integer);
alter table  casesEnsitivE set tblproperties('some'='one');
