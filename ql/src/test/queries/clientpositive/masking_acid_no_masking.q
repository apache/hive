-- Simulate the case for org.apache.ranger.authorization.hive.authorizer.RangerHiveAuthorizerFactory,
-- when all tables are marked eligible for masking. This shouldn't break any ACID operations.

set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table nonacid_n0 (key int, value string) stored as orc;

create table masking_acid_no_masking (key int, value string)
clustered by (value) into 2 buckets stored as orc
tblproperties ("transactional"="true");

update masking_acid_no_masking set key=1 where value='ddd';

delete from masking_acid_no_masking where value='ddd';

MERGE INTO masking_acid_no_masking as t using nonacid_n0 as s ON t.key = s.key
WHEN MATCHED AND s.key < 5 THEN DELETE
WHEN MATCHED AND s.key < 3 THEN UPDATE set key = 1
WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.value);
