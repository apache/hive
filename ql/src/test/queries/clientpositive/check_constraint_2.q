set hive.stats.autogather=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.optimize.reducededuplication.min.reducer = 2;

-- UPDATE
create table acid_uami_n0(i int,
                 de decimal(5,2) constraint nn1 not null enforced,
                 vc varchar(128) constraint ch2 CHECK (de >= cast(i as decimal(5,2))) enforced)
                 clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

-- update
explain update acid_uami_n0 set de = 893.14 where de = 103.00 or de = 119.00;


set hive.optimize.reducededuplication.min.reducer = 1;
-- update
explain update acid_uami_n0 set de = 893.14 where de = 103.00 or de = 119.00;
