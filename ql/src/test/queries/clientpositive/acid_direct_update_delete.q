-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.acid.direct.insert.enabled=true;

DROP TABLE IF EXISTS test_update_bucketed;

CREATE TABLE test_update_bucketed(id string, value string) CLUSTERED BY(id) INTO 10 BUCKETS STORED AS ORC TBLPROPERTIES('transactional'='true');

INSERT INTO test_update_bucketed values ('1','one'),('2','two'),('3','three'),('4','four'),('5','five'),('6','six'),('7','seven'),('8','eight'),('9','nine'),('10','ten'),('11','eleven'),('12','twelve'),('13','thirteen'),('14','fourteen'),('15','fifteen'),('16','sixteen'),('17','seventeen'),('18','eighteen'),('19','nineteen'),('20','twenty');

SELECT * FROM test_update_bucketed;
DELETE FROM test_update_bucketed WHERE id IN ('2', '4', '12', '15');
UPDATE test_update_bucketed SET value='New value' WHERE id IN ('6','11', '18', '20');
SELECT * FROM test_update_bucketed;
DELETE FROM test_update_bucketed WHERE id IN ('2', '11', '10');
UPDATE test_update_bucketed SET value='New value2' WHERE id IN ('2','18', '19');
SELECT * FROM test_update_bucketed;

DROP TABLE IF EXISTS test_update_bucketed;