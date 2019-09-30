--! qt:dataset:srcpart
set hive.mapred.mode=nonstrict;
set hive.exec.submitviachild=false;
set hive.exec.submit.local.task.via.child=false;
set hive.exec.dynamic.partition=true;

set hive.exec.reducers.max=4;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.default.fileformat=RCFILE;
set hive.exec.pre.hooks = org.apache.hadoop.hive.ql.hooks.PreExecutePrinter,org.apache.hadoop.hive.ql.hooks.EnforceReadOnlyTables,org.apache.hadoop.hive.ql.hooks.UpdateInputAccessTimeHook$PreExec;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table srcpartbucket (key string, value string) partitioned by (ds string, hr string) clustered by (key) into 4 buckets stored as orc tblproperties ("transactional"="true", "transactional_properties"="insert_only");

insert overwrite table srcpartbucket partition(ds, hr) select * from srcpart where ds is not null and key < 10;


select * from srcpartbucket;
explain select key from srcpartbucket tablesample (bucket 2 out of 4 on key);
select key from srcpartbucket tablesample (bucket 1 out of 4 on key);
select key from srcpartbucket tablesample (bucket 2 out of 4 on key);
select key from srcpartbucket tablesample (bucket 3 out of 4 on key);
select key from srcpartbucket tablesample (bucket 4 out of 4 on key);

explain
        select key from srcpartbucket tablesample (bucket 2 out of 4 on key) group by key;
select key from srcpartbucket tablesample (bucket 1 out of 4 on key) group by key;
select key from srcpartbucket tablesample (bucket 2 out of 4 on key) group by key;
select key from srcpartbucket tablesample (bucket 3 out of 4 on key) group by key;
select key from srcpartbucket tablesample (bucket 4 out of 4 on key) group by key;

