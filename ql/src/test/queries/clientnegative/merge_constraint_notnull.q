set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table nonacid (key int, a1 string, value string) stored as orc;
insert into nonacid values(1, null, 'value');

create table testT (key int NOT NULL enable, a1 string NOT NULL enforced, value string)
clustered by (value) into 2 buckets stored as orc
tblproperties ("transactional"="true");
insert into testT values(2,'a1masking', 'valuemasking');

MERGE INTO testT as t using nonacid as s ON t.key = s.key
WHEN MATCHED AND s.key > 5 THEN DELETE
WHEN MATCHED AND s.key < 3 THEN UPDATE set a1 = '1'
WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.a1, s.value);

