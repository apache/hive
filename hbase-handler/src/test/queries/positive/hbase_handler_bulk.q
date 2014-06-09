-- -*- mode:sql -*-

drop table if exists hb_target;

-- this is the target HBase table
create table hb_target(key int, val string)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties ('hbase.columns.mapping' = ':key,cf:val')
tblproperties ('hbase.table.name' = 'positive_hbase_handler_bulk');

set hive.hbase.generatehfiles=true;
set hfile.family.path=/tmp/hb_target/cf;

-- this should produce three files in /tmp/hb_target/cf
insert overwrite table hb_target select distinct key, value from src cluster by key;

-- To get the files out to your local filesystem for loading into
-- HBase, run mkdir -p /tmp/blah/cf, then uncomment and
-- semicolon-terminate the line below before running this test:
-- dfs -copyToLocal /tmp/hb_target/cf/* /tmp/blah/cf

drop table hb_target;
dfs -rmr /tmp/hb_target/cf;
