



CREATE TABLE table1 (a STRING, b STRING) STORED AS TEXTFILE;
DESCRIBE FORMATTED table1;

CREATE TABLE table2 LIKE table1;
DESCRIBE FORMATTED table2;

CREATE TABLE IF NOT EXISTS table2 LIKE table1;

CREATE EXTERNAL TABLE IF NOT EXISTS table2 LIKE table1;

CREATE EXTERNAL TABLE IF NOT EXISTS table3 LIKE table1;
DESCRIBE FORMATTED table3;

INSERT OVERWRITE TABLE table1 SELECT key, value FROM src WHERE key = 86;
INSERT OVERWRITE TABLE table2 SELECT key, value FROM src WHERE key = 100;

SELECT * FROM table1;
SELECT * FROM table2;

dfs -cp ${system:hive.root}/data/files/ext_test ${system:test.tmp.dir}/ext_test;

CREATE EXTERNAL TABLE table4 (a INT) LOCATION '${system:test.tmp.dir}/ext_test';
CREATE EXTERNAL TABLE table5 LIKE table4 LOCATION '${system:test.tmp.dir}/ext_test';

SELECT * FROM table4;
SELECT * FROM table5;

DROP TABLE table5;
SELECT * FROM table4;
DROP TABLE table4;

CREATE EXTERNAL TABLE table4 (a INT) LOCATION '${system:test.tmp.dir}/ext_test';
SELECT * FROM table4;

CREATE TABLE doctors STORED AS AVRO TBLPROPERTIES ('avro.schema.literal'='{
  "namespace": "testing.hive.avro.serde",
  "name": "doctors",
  "type": "record",
  "fields": [
    {
      "name":"number",
      "type":"int",
      "doc":"Order of playing the role"
    },
    {
      "name":"first_name",
      "type":"string",
      "doc":"first name of actor playing role"
    },
    {
      "name":"last_name",
      "type":"string",
      "doc":"last name of actor playing role"
    }
  ]
}');

alter table doctors set tblproperties ('k1'='v1', 'k2'='v2');
DESCRIBE FORMATTED doctors;

CREATE TABLE doctors2 like doctors;
DESCRIBE FORMATTED doctors2;

CREATE TABLE PropertiedParquetTable(a INT, b STRING) STORED AS PARQUET TBLPROPERTIES("parquet.compression"="LZO");
CREATE TABLE LikePropertiedParquetTable LIKE PropertiedParquetTable;

DESCRIBE FORMATTED LikePropertiedParquetTable;

CREATE TABLE table5(col1 int, col2 string) stored as TEXTFILE;
DESCRIBE FORMATTED table5;

CREATE TABLE table6 like table5 stored as RCFILE;
DESCRIBE FORMATTED table6;

drop table table6;

CREATE  TABLE table6 like table5 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat' OUTPUTFORMAT   'org.apache.hadoop.hive.ql.io.RCFileOutputFormat' LOCATION '${system:hive.root}/data/files/table6';
DESCRIBE FORMATTED table6;

drop table table5;

create table orc_table (
`time` string)
stored as ORC tblproperties ("orc.compress"="SNAPPY");

create table orc_table_using_like like orc_table;

describe formatted orc_table_using_like;

drop table orc_table_using_like;

drop table orc_table;

