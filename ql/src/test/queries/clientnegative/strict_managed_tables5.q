
set metastore.strict.managed.tables=true;

create external table strict_managed_tables5_tab1 (c1 string, c2 string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED BY 'org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler';

-- Managed non-native table should fail
create table strict_managed_tables5_tab2 (c1 string, c2 string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED BY 'org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler';

