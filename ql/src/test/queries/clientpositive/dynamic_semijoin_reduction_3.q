set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.explain.user=false;
set hive.merge.cardinality.check=true;

set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;

-- Try with merge statements
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

explain merge into acidTbl as t using (
  select * from nonAcidOrcTbl where a > 0
  union all
  select * from nonAcidOrcTbl where b > 0
) s ON t.a = s.a
WHEN MATCHED AND s.a > 8 THEN DELETE
WHEN MATCHED THEN UPDATE SET b = 7
WHEN NOT MATCHED THEN INSERT VALUES(s.a, s.b);
