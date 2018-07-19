--! qt:dataset:src
-- remove these; after HIVE-18802 is fixed
set hive.optimize.index.filter=false;
set hive.optimize.ppd=false;
-- remove these; after HIVE-18802 is fixed


DROP TABLE accumulo_table_1;
CREATE EXTERNAL TABLE accumulo_table_1(key int, value string) 
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES ("accumulo.columns.mapping" = ":rowID,cf:string")
TBLPROPERTIES ("accumulo.table.name" = "accumulo_table_0",
               "external.table.purge" = "true");

DESCRIBE EXTENDED accumulo_table_1;

select * from accumulo_table_1;

EXPLAIN FROM src INSERT OVERWRITE TABLE accumulo_table_1 SELECT * WHERE (key%2)=0;
FROM src INSERT OVERWRITE TABLE accumulo_table_1 SELECT * WHERE (key%2)=0;

DROP TABLE accumulo_table_2;
CREATE EXTERNAL TABLE accumulo_table_2(key int, value string) 
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES ("accumulo.columns.mapping" = ":rowID,cf:string")
TBLPROPERTIES ("accumulo.table.name" = "accumulo_table_0");

EXPLAIN 
SELECT Y.* 
FROM 
(SELECT accumulo_table_1.* FROM accumulo_table_1) x
JOIN 
(SELECT src.* FROM src) Y
ON (x.key = Y.key)
ORDER BY key, value LIMIT 20;

SELECT Y.* 
FROM 
(SELECT accumulo_table_1.* FROM accumulo_table_1) x
JOIN 
(SELECT src.* FROM src) Y
ON (x.key = Y.key)
ORDER BY key, value LIMIT 20;

EXPLAIN 
SELECT Y.*
FROM 
(SELECT accumulo_table_1.* FROM accumulo_table_1 WHERE 100 < accumulo_table_1.key) x
JOIN 
(SELECT accumulo_table_2.* FROM accumulo_table_2 WHERE accumulo_table_2.key < 120) Y
ON (x.key = Y.key)
ORDER BY key, value;

SELECT Y.*
FROM 
(SELECT accumulo_table_1.* FROM accumulo_table_1 WHERE 100 < accumulo_table_1.key) x
JOIN 
(SELECT accumulo_table_2.* FROM accumulo_table_2 WHERE accumulo_table_2.key < 120) Y
ON (x.key = Y.key)
ORDER BY key,value;

DROP TABLE empty_accumulo_table;
CREATE EXTERNAL TABLE empty_accumulo_table(key int, value string) 
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES ("accumulo.columns.mapping" = ":rowID,cf:string")
TBLPROPERTIES ("external.table.purge" = "true");

DROP TABLE empty_normal_table;
CREATE TABLE empty_normal_table(key int, value string);

select * from (select count(1) as c from empty_normal_table union all select count(1) as c from empty_accumulo_table) x order by c;
select * from (select count(1) c from empty_normal_table union all select count(1) as c from accumulo_table_1) x order by c;
select * from (select count(1) c from src union all select count(1) as c from empty_accumulo_table) x order by c;
select * from (select count(1) c from src union all select count(1) as c from accumulo_table_1) x order by c;

CREATE EXTERNAL TABLE accumulo_table_3(key int, value string, count int) 
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES (
"accumulo.columns.mapping" = ":rowID,cf:val,cf2:count"
)
TBLPROPERTIES ("external.table.purge" = "true");

EXPLAIN 
INSERT OVERWRITE TABLE accumulo_table_3
SELECT x.key, x.value, Y.count 
FROM 
(SELECT accumulo_table_1.* FROM accumulo_table_1) x
JOIN 
(SELECT src.key, count(src.key) as count FROM src GROUP BY src.key) Y
ON (x.key = Y.key);

INSERT OVERWRITE TABLE accumulo_table_3
SELECT x.key, x.value, Y.count 
FROM 
(SELECT accumulo_table_1.* FROM accumulo_table_1) x
JOIN 
(SELECT src.key, count(src.key) as count FROM src GROUP BY src.key) Y
ON (x.key = Y.key);

select count(1) from accumulo_table_3;
select * from accumulo_table_3 order by key, value limit 5;
select key, count from accumulo_table_3 order by key, count desc limit 5;

DROP TABLE accumulo_table_4;
CREATE EXTERNAL TABLE accumulo_table_4(key int, value1 string, value2 int, value3 int) 
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES (
"accumulo.columns.mapping" = ":rowID,a:b,a:c,d:e"
)
TBLPROPERTIES ("external.table.purge" = "true");

INSERT OVERWRITE TABLE accumulo_table_4 SELECT key, value, key+1, key+2 
FROM src WHERE key=98 OR key=100;

SELECT * FROM accumulo_table_4 ORDER BY key;

DROP TABLE accumulo_table_5;
CREATE EXTERNAL TABLE accumulo_table_5(key int, value map<string,string>) 
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES ("accumulo.columns.mapping" = ":rowID,a:*")
TBLPROPERTIES ("accumulo.table.name" = "accumulo_table_4");

SELECT * FROM accumulo_table_5 ORDER BY key;

DROP TABLE accumulo_table_6;
CREATE EXTERNAL TABLE accumulo_table_6(key int, value map<string,string>) 
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES (
"accumulo.columns.mapping" = ":rowID,cf:*"
)
TBLPROPERTIES ("external.table.purge" = "true");
INSERT OVERWRITE TABLE accumulo_table_6 SELECT key, map(value, key) FROM src
WHERE key=98 OR key=100;

SELECT * FROM accumulo_table_6 ORDER BY key;

DROP TABLE accumulo_table_7;
CREATE EXTERNAL TABLE accumulo_table_7(value map<string,string>, key int) 
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES (
"accumulo.columns.mapping" = "cf:*,:rowID"
)
TBLPROPERTIES ("external.table.purge" = "true");
INSERT OVERWRITE TABLE accumulo_table_7 
SELECT map(value, key, upper(value), key+1), key FROM src
WHERE key=98 OR key=100;

SELECT * FROM accumulo_table_7 ORDER BY key;

DROP TABLE accumulo_table_8;
CREATE EXTERNAL TABLE accumulo_table_8(key int, value1 string, value2 int, value3 int) 
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES (
"accumulo.columns.mapping" = ":rowID,a:b,a:c,d:e"
)
TBLPROPERTIES ("external.table.purge" = "true");

INSERT OVERWRITE TABLE accumulo_table_8 SELECT key, value, key+1, key+2 
FROM src WHERE key=98 OR key=100;

SELECT * FROM accumulo_table_8 ORDER BY key;

DROP TABLE accumulo_table_1;
DROP TABLE accumulo_table_2;
DROP TABLE accumulo_table_3;
DROP TABLE accumulo_table_4;
DROP TABLE accumulo_table_5;
DROP TABLE accumulo_table_6;
DROP TABLE accumulo_table_7;
DROP TABLE accumulo_table_8;
DROP TABLE empty_accumulo_table;
DROP TABLE empty_normal_table;
