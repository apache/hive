DROP TABLE hbase_table_1;
CREATE EXTERNAL TABLE hbase_table_1(key int comment 'It is a column key', value string comment 'It is the column string value')
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = "cf:string")
TBLPROPERTIES ("hbase.table.name" = "hbase_table_0", "external.table.purge" = "true");

DESCRIBE EXTENDED hbase_table_1;

select * from hbase_table_1;

EXPLAIN FROM src INSERT OVERWRITE TABLE hbase_table_1 SELECT * WHERE (key%2)=0;
FROM src INSERT OVERWRITE TABLE hbase_table_1 SELECT * WHERE (key%2)=0;

DROP TABLE hbase_table_2;
CREATE EXTERNAL TABLE hbase_table_2(key int, value string) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = "cf:string")
TBLPROPERTIES ("hbase.table.name" = "hbase_table_0");

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
(SELECT hbase_table_1.* FROM hbase_table_1 WHERE 100 < hbase_table_1.key) x
JOIN 
(SELECT hbase_table_2.* FROM hbase_table_2 WHERE hbase_table_2.key < 120) Y
ON (x.key = Y.key)
ORDER BY key, value;

SELECT Y.*
FROM 
(SELECT hbase_table_1.* FROM hbase_table_1 WHERE 100 < hbase_table_1.key) x
JOIN 
(SELECT hbase_table_2.* FROM hbase_table_2 WHERE hbase_table_2.key < 120) Y
ON (x.key = Y.key)
ORDER BY key,value;

DROP TABLE empty_hbase_table;
CREATE EXTERNAL TABLE empty_hbase_table(key int, value string) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = "cf:string")
TBLPROPERTIES ("external.table.purge" = "true");

DROP TABLE empty_normal_table;
CREATE TABLE empty_normal_table(key int, value string);

select * from (select count(1) as c from empty_normal_table union all select count(1) as c from empty_hbase_table) x order by c;
select * from (select count(1) c from empty_normal_table union all select count(1) as c from hbase_table_1) x order by c;
select * from (select count(1) c from src union all select count(1) as c from empty_hbase_table) x order by c;
select * from (select count(1) c from src union all select count(1) as c from hbase_table_1) x order by c;

CREATE EXTERNAL TABLE hbase_table_3(key int, value string, count int) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = "cf:val,cf2:count"
)
TBLPROPERTIES ("external.table.purge" = "true");

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
CREATE EXTERNAL TABLE hbase_table_4(key int, value1 string, value2 int, value3 int) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = "a:b,a:c,d:e"
)
TBLPROPERTIES ("external.table.purge" = "true");

INSERT OVERWRITE TABLE hbase_table_4 SELECT key, value, key+1, key+2 
FROM src WHERE key=98 OR key=100;

SELECT * FROM hbase_table_4 ORDER BY key;

DROP TABLE hbase_table_5;
CREATE EXTERNAL TABLE hbase_table_5(key int, value map<string,string>) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = "a:")
TBLPROPERTIES ("hbase.table.name" = "hbase_table_4");

SELECT * FROM hbase_table_5 ORDER BY key;

DROP TABLE hbase_table_6;
CREATE EXTERNAL TABLE hbase_table_6(key int, value map<string,string>) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = ":key,cf:"
)
TBLPROPERTIES ("external.table.purge" = "true");
INSERT OVERWRITE TABLE hbase_table_6 SELECT key, map(value, key) FROM src
WHERE key=98 OR key=100;

SELECT * FROM hbase_table_6 ORDER BY key;

DROP TABLE hbase_table_7;
CREATE EXTERNAL TABLE hbase_table_7(value map<string,string>, key int) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = "cf:,:key"
)
TBLPROPERTIES ("external.table.purge" = "true");
INSERT OVERWRITE TABLE hbase_table_7 
SELECT map(value, key, upper(value), key+1), key FROM src
WHERE key=98 OR key=100;

SELECT * FROM hbase_table_7 ORDER BY key;

set hive.hbase.wal.enabled=false;

DROP TABLE hbase_table_8;
CREATE EXTERNAL TABLE hbase_table_8(key int, value1 string, value2 int, value3 int) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = "a:b,a:c,d:e"
)
TBLPROPERTIES ("external.table.purge" = "true");

INSERT OVERWRITE TABLE hbase_table_8 SELECT key, value, key+1, key+2 
FROM src WHERE key=98 OR key=100;

SELECT * FROM hbase_table_8 ORDER BY key;

DROP TABLE IF EXISTS hbase_table_3_like;
CREATE TABLE hbase_table_3_like LIKE hbase_table_3;
DESCRIBE EXTENDED hbase_table_3_like;

INSERT OVERWRITE TABLE hbase_table_3_like SELECT * FROM hbase_table_3;
SELECT * FROM hbase_table_3_like ORDER BY key, value LIMIT 5;

DROP TABLE IF EXISTS hbase_table_1_like;
CREATE EXTERNAL TABLE hbase_table_1_like LIKE hbase_table_1;
DESCRIBE EXTENDED hbase_table_1_like;

INSERT OVERWRITE TABLE hbase_table_1_like SELECT * FROM hbase_table_1;
SELECT COUNT(*) FROM hbase_table_1_like;

SHOW CREATE TABLE hbase_table_1_like;

DROP TABLE IF EXISTS hbase_table_9;
CREATE EXTERNAL TABLE hbase_table_9 (id bigint, data map<string, string>, str string)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties ("hbase.columns.mapping" = ":key,cf:map_col#s:s,cf:str_col")
TBLPROPERTIES ("external.table.purge" = "true");

insert overwrite table hbase_table_9 select 1 as id, map('abcd', null) as data , null as str from src limit 1;
insert into table hbase_table_9 select 2 as id, map('efgh', null) as data , '1234' as str from src limit 1;
insert into table hbase_table_9 select 3 as id, map('hij', '') as data , '1234' as str from src limit 1;
insert into table hbase_table_9 select 4 as id, map('klm', 'avalue') as data , '1234' as str from src limit 1;
insert into table hbase_table_9 select 5 as id, map('key1',null, 'key2', 'avalue') as data , '1234' as str from src limit 1;
select * from hbase_table_9;

DROP TABLE IF EXISTS hbase_table_10;
CREATE EXTERNAL TABLE hbase_table_10 (id bigint, data map<int, int>, str string)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties ("hbase.columns.mapping" = ":key,cf:map_col2,cf:str2_col")
TBLPROPERTIES ("external.table.purge" = "true");
set hive.cbo.enable=false;
insert overwrite table hbase_table_10 select 1 as id, map(10, cast(null as int)) as data , null as str from src limit 1;
insert into table hbase_table_10 select 2 as id, map(20, cast(null as int)) as data , '1234' as str from src limit 1;
insert into table hbase_table_10 select 3 as id, map(30, 31) as data , '1234' as str from src limit 1;
insert into table hbase_table_10 select 4 as id, map(40, cast(null as int), 45, cast(null as int)) as data , '1234' as str from src limit 1;
insert into table hbase_table_10 select 5 as id, map(50,cast(null as int), 55, 58) as data , '1234' as str from src limit 1;
select * from hbase_table_10;


DROP TABLE IF EXISTS hbase_table_11;
CREATE EXTERNAL TABLE hbase_table_11(id INT, map_column STRUCT<s_int:INT,s_string:STRING,s_date:DATE>)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping'=':key,id:id')
TBLPROPERTIES ("external.table.purge" = "true");
INSERT INTO hbase_table_11 SELECT 2,NAMED_STRUCT("s_int",CAST(NULL AS INT),"s_string","s1","s_date",CAST('2018-03-12' AS DATE)) FROM src LIMIT 1;
select * from hbase_table_11;

DROP TABLE IF EXISTS hbase_table_12;
CREATE EXTERNAL TABLE hbase_table_12(id INT, list_column ARRAY <STRING>)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping'=':key,id:id')
TBLPROPERTIES ("external.table.purge" = "true");
INSERT INTO hbase_table_12 SELECT 2, ARRAY("a", CAST (NULL AS STRING),  "b") FROM src LIMIT 1;
select * from hbase_table_12;

DROP TABLE hbase_table_1;
DROP TABLE hbase_table_1_like;
DROP TABLE hbase_table_2;
DROP TABLE hbase_table_3;
DROP TABLE hbase_table_3_like;
DROP TABLE hbase_table_4;
DROP TABLE hbase_table_5;
DROP TABLE hbase_table_6;
DROP TABLE hbase_table_7;
DROP TABLE hbase_table_8;
DROP TABLE empty_hbase_table;
DROP TABLE empty_normal_table;
DROP TABLE hbase_table_9;
DROP TABLE hbase_table_10;
DROP TABLE hbase_table_11;
DROP TABLE hbase_table_12;
