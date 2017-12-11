set hive.stats.column.autogather=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.mode.local.auto=false;

set mapred.reduce.tasks = 2;

-- This test sets number of mapred tasks to 2 for a table with 4 buckets,
-- and uses a post-hook to confirm that 1 tasks were created

drop table if exists bucket_nr_acid2;
create table bucket_nr_acid2 (a int, b int) clustered by (a) into 4 buckets stored as orc TBLPROPERTIES ('transactional'='true');
set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyNumReducersHook;
set VerifyNumReducersHook.num.reducers=2;

-- txn X write to b0 + b1
insert into bucket_nr_acid2 values(0,1),(1,1);
-- txn X + 1 write to b2 + b3
insert into bucket_nr_acid2 values(2,2),(3,2);
-- txn X + 2 write to b0 + b1
insert into bucket_nr_acid2 values(0,3),(1,3);
-- txn X + 3 write to b2 + b3
insert into bucket_nr_acid2 values(2,4),(3,4);

-- so with 2 FileSinks and 4 buckets, FS1 should see (0,1),(2,2),(0,3)(2,4) since data is sorted by
-- ROW__ID where tnxid is the first component FS2 should see (1,1),(3,2),(1,3),(3,4)


update bucket_nr_acid2 set b = -1;
set hive.exec.post.hooks=;
select * from bucket_nr_acid2 order by a, b;

drop table bucket_nr_acid2;

