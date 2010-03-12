DROP TABLE hbase_table_1;
CREATE TABLE hbase_table_1(key int, value string) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = "cf:string",
"hbase.table.name" = "hbase_table_0"
);

DESCRIBE EXTENDED hbase_table_1;

select * from hbase_table_1;

EXPLAIN FROM src INSERT OVERWRITE TABLE hbase_table_1 SELECT *;
FROM src INSERT OVERWRITE TABLE hbase_table_1 SELECT *;

DROP TABLE hbase_table_2;
CREATE EXTERNAL TABLE hbase_table_2(key int, value string) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = "cf:string",
"hbase.table.name" = "hbase_table_0"
);

EXPLAIN 
SELECT Y.* 
FROM 
(SELECT hbase_table_1.* FROM hbase_table_1) x
JOIN 
(SELECT src.* FROM src) Y
ON (x.key = Y.key)
ORDER BY key, value LIMIT 20;

SELECT Y.* 
FROM 
(SELECT hbase_table_1.* FROM hbase_table_1) x
JOIN 
(SELECT src.* FROM src) Y
ON (x.key = Y.key)
ORDER BY key, value LIMIT 20;

EXPLAIN 
SELECT Y.*
FROM 
(SELECT hbase_table_1.* FROM hbase_table_1 WHERE hbase_table_1.key > 100) x
JOIN 
(SELECT hbase_table_2.* FROM hbase_table_2 WHERE hbase_table_2.key < 120) Y
ON (x.key = Y.key)
ORDER BY key, value;

SELECT Y.*
FROM 
(SELECT hbase_table_1.* FROM hbase_table_1 WHERE hbase_table_1.key > 100) x
JOIN 
(SELECT hbase_table_2.* FROM hbase_table_2 WHERE hbase_table_2.key < 120) Y
ON (x.key = Y.key)
ORDER BY key,value;

DROP TABLE empty_hbase_table;
CREATE TABLE empty_hbase_table(key int, value string) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = "cf:string"
);

DROP TABLE empty_normal_table;
CREATE TABLE empty_normal_table(key int, value string);

select * from (select count(1) as c from empty_normal_table union all select count(1) as c from empty_hbase_table) x order by c;
select * from (select count(1) c from empty_normal_table union all select count(1) as c from hbase_table_1) x order by c;
select * from (select count(1) c from src union all select count(1) as c from empty_hbase_table) x order by c;
select * from (select count(1) c from src union all select count(1) as c from hbase_table_1) x order by c;

CREATE TABLE hbase_table_3(key int, value string, count int) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = "cf:val,cf2:count"
);

EXPLAIN 
INSERT OVERWRITE TABLE hbase_table_3
SELECT x.key, x.value, Y.count 
FROM 
(SELECT hbase_table_1.* FROM hbase_table_1) x
JOIN 
(SELECT src.key, count(src.key) as count FROM src GROUP BY src.key) Y
ON (x.key = Y.key);

INSERT OVERWRITE TABLE hbase_table_3
SELECT x.key, x.value, Y.count 
FROM 
(SELECT hbase_table_1.* FROM hbase_table_1) x
JOIN 
(SELECT src.key, count(src.key) as count FROM src GROUP BY src.key) Y
ON (x.key = Y.key);

select count(1) from hbase_table_3;
select * from hbase_table_3 order by key, value limit 5;
select key, count from hbase_table_3 order by key, count desc limit 5;

DROP TABLE hbase_table_4;
CREATE TABLE hbase_table_4(key int, value1 string, value2 int, value3 int) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = "a:b,a:c,d:e"
);

INSERT OVERWRITE TABLE hbase_table_4 SELECT key, value, key+1, key+2 
FROM src WHERE key=98 OR key=100;

SELECT * FROM hbase_table_4 ORDER BY key;

DROP TABLE hbase_table_5;
CREATE EXTERNAL TABLE hbase_table_5(key int, value map<string,string>) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = "a:",
"hbase.table.name" = "hbase_table_4"
);

SELECT * FROM hbase_table_5 ORDER BY key;

DROP TABLE hbase_table_6;
CREATE TABLE hbase_table_6(key int, value map<string,string>) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = "cf:"
);
INSERT OVERWRITE TABLE hbase_table_6 SELECT key, map(value, key) FROM src
WHERE key=98 OR key=100;

SELECT * FROM hbase_table_6 ORDER BY key;

DROP TABLE hbase_table_1;
DROP TABLE hbase_table_2;
DROP TABLE hbase_table_3;
DROP TABLE hbase_table_4;
DROP TABLE hbase_table_5;
DROP TABLE hbase_table_6;
DROP TABLE empty_hbase_table;
DROP TABLE empty_normal_table;
