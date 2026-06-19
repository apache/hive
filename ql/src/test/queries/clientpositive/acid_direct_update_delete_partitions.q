SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.acid.direct.insert.enabled=true;

DROP TABLE IF EXISTS test_update_part_text;
DROP TABLE IF EXISTS test_update_part;
DROP TABLE IF EXISTS test_delete_part;

CREATE EXTERNAL TABLE test_update_part_text (a int, b int, c int) STORED AS TEXTFILE;
INSERT INTO test_update_part_text VALUES (11, 1, 11), (12, 2, 11), (13, 3, 11), (14, 4, 11), (14, 5, NULL), (15, 6, NULL), (16, 7, NULL), (14, 8, 22), (17, 8, 22), (18, 9, 22), (19, 10, 33), (20, 11, 33), (21, 12, 33);

CREATE TABLE test_update_part (a int, b int) PARTITIONED BY (c int) CLUSTERED BY(a) INTO 4 BUCKETS STORED AS ORC TBLPROPERTIES('transactional'='true');
INSERT OVERWRITE TABLE test_update_part SELECT a, b, c FROM test_update_part_text;

SELECT * FROM test_update_part ORDER BY c, b, a;

UPDATE test_update_part SET b=1111 WHERE b=8;
UPDATE test_update_part SET b=2222 WHERE a=8;
UPDATE test_update_part SET b=3333 WHERE a=14;
UPDATE test_update_part SET b=4444 WHERE c is null;
UPDATE test_update_part SET b=5555 WHERE c=33;
UPDATE test_update_part SET b=6666 WHERE a IN (SELECT a FROM test_update_part_text WHERE (c=11 and b=2) or c=33);
SELECT * FROM test_update_part ORDER BY c, b, a;

CREATE TABLE test_delete_part (a int, b int) PARTITIONED BY (c int) STORED AS ORC TBLPROPERTIES('transactional'='true');
INSERT OVERWRITE TABLE test_delete_part SELECT a, b, c FROM test_update_part_text;

SELECT * FROM test_delete_part order by c, b, a;

DELETE FROM test_delete_part WHERE b=8;
DELETE FROM test_delete_part WHERE a=8;
DELETE FROM test_delete_part WHERE b=8;
DELETE FROM test_delete_part WHERE a=14;
DELETE FROM test_delete_part WHERE c is null;
DELETE FROM test_delete_part WHERE c=33;
DELETE FROM test_delete_part WHERE a in (SELECT a FROM test_update_part_text WHERE (c=11 and b=2) or c=22);
SELECT * FROM test_delete_part order by c, b, a;

DROP TABLE IF EXISTS test_update_part_text;
DROP TABLE IF EXISTS test_update_part;
DROP TABLE IF EXISTS test_delete_part;