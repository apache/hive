set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set metastore.strict.managed.tables=true;

-- Managed transactional table ok
create table strict_managed_tables1_tab1 (c1 string, c2 string) stored as orc tblproperties ('transactional'='true');

-- Managed insert-only transactional table ok
create table strict_managed_tables1_tab2 (c1 string, c2 string) stored as textfile tblproperties ('transactional'='true', 'transactional_properties'='insert_only');

-- External non-transactional table ok
create external table strict_managed_tables1_tab3 (c1 string, c2 string) stored as textfile;

-- Managed non-transactional table not ok
create table strict_managed_tables1_tab4 (c1 string, c2 string) stored as textfile;
