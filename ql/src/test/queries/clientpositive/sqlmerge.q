set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.explain.user=false;
set hive.merge.cardinality.check=true;

create table acidTbl(a int, b int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
create table nonAcidOrcTbl(a int, b int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='false');

--expect a cardinality check because there is update and hive.merge.cardinality.check=true by default
explain merge into acidTbl as t using nonAcidOrcTbl s ON t.a = s.a 
WHEN MATCHED AND s.a > 8 THEN DELETE
WHEN MATCHED THEN UPDATE SET b = 7
WHEN NOT MATCHED THEN INSERT VALUES(s.a, s.b);

--now we expect no cardinality check since only have insert clause
explain merge into acidTbl as t using nonAcidOrcTbl s ON t.a = s.a
WHEN NOT MATCHED THEN INSERT VALUES(s.a, s.b);
