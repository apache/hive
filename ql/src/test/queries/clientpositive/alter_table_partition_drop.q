set hive.exec.dynamic.partition=true;  
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS part_table PURGE;
CREATE TABLE part_table (key INT, value STRING) partitioned by (p STRING);

-- Note that HIVE-9481 allow column list specification in INSERT statement
-- is not in CDH. So we are working around that in CDH.
-- INSERT INTO part_table PARTITION(p)(p,key,value) values('2014-09-23', 1, 'foo'),('2014-09-24', 2, 'bar');
INSERT INTO part_table PARTITION(p) select 1 as key, 'foo' as value, '2014-09-23' as p;
INSERT INTO part_table PARTITION(p) select 2 as key, 'bar' as value, '2014-09-24' as p;
SELECT * FROM part_table;
ALTER TABLE part_table DROP PARTITION (p='2014-09-23');
SELECT * FROM part_table;
ALTER TABLE part_table DROP PARTITION (p='2014-09-24') PURGE;
SELECT * FROM part_table;
