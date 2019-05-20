set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.mode.local.auto=false;

set mapred.reduce.tasks = 1;

-- This test sets number of mapred tasks to 1 for a table with 2 buckets,
-- and uses a post-hook to confirm that 1 tasks were created

drop table if exists bucket_nr_acid;
create table bucket_nr_acid (a int, b int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyNumReducersHook;
set VerifyNumReducersHook.num.reducers=1;

-- txn X write to b1
insert into bucket_nr_acid values(1,1);
-- txn X + 1 write to bucket0 + b1
insert into bucket_nr_acid values(0,0),(3,3);

update bucket_nr_acid set b = -1;
set hive.exec.post.hooks=;
select * from bucket_nr_acid order by a, b;

drop table bucket_nr_acid;






