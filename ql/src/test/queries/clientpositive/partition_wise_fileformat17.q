-- HIVE-5199 : CustomSerDe1 and CustomSerDe2 are used here.
-- The final results should be all NULL columns deserialized using 
-- CustomSerDe1 and CustomSerDe2 irrespective of the inserted values

DROP TABLE PW17;
ADD JAR ../build/ql/test/test-serdes.jar;
CREATE TABLE PW17(USER STRING, COMPLEXDT ARRAY<INT>) PARTITIONED BY (YEAR STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.CustomSerDe1';
LOAD DATA LOCAL INPATH '../data/files/pw17.txt' INTO TABLE PW17 PARTITION (YEAR='1');
ALTER TABLE PW17 PARTITION(YEAR='1') SET SERDE 'org.apache.hadoop.hive.serde2.CustomSerDe2';
ALTER TABLE PW17 SET SERDE 'org.apache.hadoop.hive.serde2.CustomSerDe1';
-- Without the fix, will throw cast exception via FetchOperator
SELECT * FROM PW17;

-- Test for non-parititioned table. 
DROP TABLE PW17_2;
CREATE TABLE PW17_2(USER STRING, COMPLEXDT ARRAY<INT>) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.CustomSerDe1';
LOAD DATA LOCAL INPATH '../data/files/pw17.txt' INTO TABLE PW17_2;
-- Without the fix, will throw cast exception via MapOperator
SELECT COUNT(*) FROM PW17_2;