set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.create.as.acid=true;
set hive.default.fileformat.managed=ORC;
set hive.metastore.client.capabilities=HIVEFULLACIDWRITE,HIVEMANAGEDINSERTWRITE;
set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;

-- Should create translated external table when transactional=false
create table translated_table1 (i int) tblproperties('transactional'='false');
desc formatted translated_table1;

-- Should create translated external table when transactional=false and have transactional_properties
create table translated_table2 (i int) tblproperties('transactional'='false', 'transactional_properties'='default');
desc formatted translated_table2;

create table translated_table3 (i int) tblproperties('transactional'='false', 'transactional_properties'='insert_only');
desc formatted translated_table3;