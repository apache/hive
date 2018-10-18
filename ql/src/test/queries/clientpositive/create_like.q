--! qt:dataset:src




CREATE TABLE table1_n17 (a STRING, b STRING) STORED AS TEXTFILE;
DESCRIBE FORMATTED table1_n17;

CREATE TABLE table2_n12 LIKE table1_n17;
DESCRIBE FORMATTED table2_n12;

CREATE TABLE IF NOT EXISTS table2_n12 LIKE table1_n17;

CREATE EXTERNAL TABLE IF NOT EXISTS table2_n12 LIKE table1_n17;

CREATE EXTERNAL TABLE IF NOT EXISTS table3_n3 LIKE table1_n17;
DESCRIBE FORMATTED table3_n3;

INSERT OVERWRITE TABLE table1_n17 SELECT key, value FROM src WHERE key = 86;
INSERT OVERWRITE TABLE table2_n12 SELECT key, value FROM src WHERE key = 100;

SELECT * FROM table1_n17;
SELECT * FROM table2_n12;

dfs -cp ${system:hive.root}/data/files/ext_test ${system:test.tmp.dir}/ext_test;

CREATE EXTERNAL TABLE table4_n1 (a INT) LOCATION '${system:test.tmp.dir}/ext_test';
CREATE EXTERNAL TABLE table5_n5 LIKE table4_n1 LOCATION '${system:test.tmp.dir}/ext_test';

SELECT * FROM table4_n1;
SELECT * FROM table5_n5;

DROP TABLE table5_n5;
SELECT * FROM table4_n1;
DROP TABLE table4_n1;

CREATE EXTERNAL TABLE table4_n1 (a INT) LOCATION '${system:test.tmp.dir}/ext_test';
SELECT * FROM table4_n1;

CREATE TABLE doctors_n2 STORED AS AVRO TBLPROPERTIES ('avro.schema.literal'='{
  "namespace": "testing.hive.avro.serde",
  "name": "doctors_n2",
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

alter table doctors_n2 set tblproperties ('k1'='v1', 'k2'='v2');
DESCRIBE FORMATTED doctors_n2;

CREATE TABLE doctors2 like doctors_n2;
DESCRIBE FORMATTED doctors2;

CREATE TABLE PropertiedParquetTable(a INT, b STRING) STORED AS PARQUET TBLPROPERTIES("parquet.compression"="LZO");
CREATE TABLE LikePropertiedParquetTable LIKE PropertiedParquetTable;

DESCRIBE FORMATTED LikePropertiedParquetTable;

CREATE TABLE table5_n5(col1 int, col2 string) stored as TEXTFILE;
DESCRIBE FORMATTED table5_n5;

CREATE TABLE table6_n4 like table5_n5 stored as RCFILE;
DESCRIBE FORMATTED table6_n4;

drop table table6_n4;

CREATE  TABLE table6_n4 like table5_n5 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat' OUTPUTFORMAT   'org.apache.hadoop.hive.ql.io.RCFileOutputFormat' LOCATION '${system:hive.root}/data/files/table6';
DESCRIBE FORMATTED table6_n4;

drop table table5_n5;

create table orc_table_n0 (
`time` string)
stored as ORC tblproperties ("orc.compress"="SNAPPY");

create table orc_table_using_like like orc_table_n0;

describe formatted orc_table_using_like;

drop table orc_table_using_like;

drop table orc_table_n0;

