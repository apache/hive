



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

CREATE EXTERNAL TABLE table4 (a INT) LOCATION '${system:hive.root}/data/files/ext_test';
CREATE EXTERNAL TABLE table5 LIKE table4 LOCATION '${system:hive.root}/data/files/ext_test';

SELECT * FROM table4;
SELECT * FROM table5;

DROP TABLE table5;
SELECT * FROM table4;
DROP TABLE table4;

CREATE EXTERNAL TABLE table4 (a INT) LOCATION '${system:hive.root}/data/files/ext_test';
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
