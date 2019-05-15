DROP TABLE src_null;
DROP TABLE hbase_null;

CREATE TABLE src_null(a STRING, b STRING, c STRING, d STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/null.txt' INTO TABLE src_null;

CREATE EXTERNAL TABLE hbase_null(key string, col1 string, col2 string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = ":key,cf1:c1,cf1:c2"
)
TBLPROPERTIES ("external.table.purge" = "true");

SELECT d, a, c FROM src_null;

INSERT INTO TABLE hbase_null SELECT d, a, c FROM src_null;

SELECT COUNT(d) FROM src_null;
SELECT COUNT(key) FROM hbase_null;
SELECT COUNT(*) FROM hbase_null;

DROP TABLE src_null;
DROP TABLE hbase_null;
