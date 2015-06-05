set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS part_table PURGE;
CREATE TABLE part_table (key INT, value STRING) partitioned by (p STRING);

INSERT INTO part_table PARTITION(p)(p,key,value) values('2014-09-23', 1, 'foo'),('2014-09-24', 2, 'bar');
SELECT * FROM part_table;
ALTER TABLE part_table DROP PARTITION (p='2014-09-23');
SELECT * FROM part_table;
ALTER TABLE part_table DROP PARTITION (p='2014-09-24') PURGE;
SELECT * FROM part_table;
