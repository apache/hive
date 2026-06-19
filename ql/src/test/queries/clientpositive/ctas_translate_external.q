set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;

set hive.fetch.task.conversion=none;
set hive.compute.query.using.stats=false;

-- CTAS with external legacy config
set hive.create.as.external.legacy=true;
create table ctas_ext_table AS SELECT from_unixtime(unix_timestamp("0002-01-01 09:57:21", "yyyy-MM-dd HH:mm:ss"));
describe formatted ctas_ext_table;

-- CTAS with transactional config set to false
set hive.create.as.external.legacy=false;
create table ctas_ext_transactional TBLPROPERTIES ('transactional'='false') AS SELECT from_unixtime(unix_timestamp("0002-01-01 09:57:21", "yyyy-MM-dd HH:mm:ss"));
describe formatted ctas_ext_transactional;
