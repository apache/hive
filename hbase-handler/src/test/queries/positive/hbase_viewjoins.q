DROP VIEW IF EXISTS VIEW_HBASE_TABLE_TEST_2;
DROP VIEW IF EXISTS VIEW_HBASE_TABLE_TEST_1;
DROP TABLE IF EXISTS HBASE_TABLE_TEST_2;
DROP TABLE IF EXISTS HBASE_TABLE_TEST_1;
CREATE TABLE HBASE_TABLE_TEST_1(
  cvalue string ,
  pk string,
 ccount int   )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  'hbase.columns.mapping'='cf:val,:key,cf2:count',
  'hbase.scan.cache'='500',
  'hbase.scan.cacheblocks'='false',
  'serialization.format'='1')
TBLPROPERTIES (
  'hbase.table.name'='hbase_table_test_1',
  'serialization.null.format'=''  );

CREATE VIEW VIEW_HBASE_TABLE_TEST_1 AS SELECT hbase_table_test_1.cvalue,hbase_table_test_1.pk,hbase_table_test_1.ccount FROM hbase_table_test_1 WHERE hbase_table_test_1.ccount IS NOT NULL;

CREATE TABLE HBASE_TABLE_TEST_2(
  cvalue string ,
    pk string ,
   ccount int  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  'hbase.columns.mapping'='cf:val,:key,cf2:count',
  'hbase.scan.cache'='500',
  'hbase.scan.cacheblocks'='false',
  'serialization.format'='1')
TBLPROPERTIES (
  'hbase.table.name'='hbase_table_test_2',
  'serialization.null.format'='');

CREATE VIEW VIEW_HBASE_TABLE_TEST_2 AS SELECT hbase_table_test_2.cvalue,hbase_table_test_2.pk,hbase_table_test_2.ccount
FROM hbase_table_test_2 WHERE  hbase_table_test_2.pk >='3-0000h-0' AND hbase_table_test_2.pk <= '3-0000h-g' AND
hbase_table_test_2.ccount IS NOT NULL;

set hive.auto.convert.join=false;

SELECT  p.cvalue cvalue
FROM `VIEW_HBASE_TABLE_TEST_1` `p`
LEFT OUTER JOIN `VIEW_HBASE_TABLE_TEST_2` `A1`
ON `p`.cvalue = `A1`.cvalue
LEFT OUTER JOIN `VIEW_HBASE_TABLE_TEST_1` `A2`
ON `p`.cvalue = `A2`.cvalue;
DROP VIEW VIEW_HBASE_TABLE_TEST_2;
DROP VIEW VIEW_HBASE_TABLE_TEST_1;
DROP TABLE HBASE_TABLE_TEST_2;
DROP TABLE HBASE_TABLE_TEST_1;
