add jar ../build/contrib/hive_contrib.jar;

DROP TABLE table1;

CREATE TABLE table1 (a STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.TypedBytesSerDe' STORED AS SEQUENCEFILE;

INSERT OVERWRITE TABLE table1 SELECT NULL FROM SRC;

SELECT * FROM table1;

SELECT a FROM table1 WHERE a IS NULL;

SELECT a FROM table1 WHERE a IS NOT NULL;

DROP TABLE table1;

