-- Tests the copying over of Table Parameters according to a HiveConf setting
-- set DDL_CTL_PARAMETERS_WHITELIST
-- when doing a CREATE TABLE LIKE.

CREATE EXTERNAL TABLE dummy_table_1 (dummy_col STRING)
STORED AS AVRO
TBLPROPERTIES (
  'avro.schema.literal'='{
    "type":"record",
    "name":"dummy_record_1",
    "namespace":"dummy_namespace",
    "fields":[
      {"name":"field1","type":["null","string"],"default":null},
      {"name":"field2","type":["null",{"type":"bytes","logicalType":"decimal","precision":10,"scale":3}],"default":null}
    ]}'
);

SHOW CREATE TABLE dummy_table_1;
CREATE EXTERNAL TABLE dummy_like_table_1 LIKE dummy_table_1;
SHOW CREATE TABLE dummy_like_table_1;

ALTER TABLE dummy_table_1 SET TBLPROPERTIES (
  'avro.schema.literal'='{
    "type":"record",
    "name":"dummy_record_1",
    "namespace":"dummy_namespace",
    "fields":[
      {"name":"field1","type":["null","string"],"default":null},
      {"name":"field2","type":["null",{"type":"bytes","logicalType":"decimal","precision":10,"scale":3}],"default":null},
      {"name":"field3","type":["null","string"],"default":null}
    ]}'
);

SHOW CREATE TABLE dummy_table_1;
CREATE EXTERNAL TABLE dummy_like_table_2 LIKE dummy_table_1;
SHOW CREATE TABLE dummy_like_table_2;

DROP TABLE dummy_like_table_1;
DROP TABLE dummy_like_table_2;
DROP TABLE dummy_table_1;

SET hive.ddl.createtablelike.properties.whitelist=avro.schema.literal,avro.schema.url;


CREATE EXTERNAL TABLE dummy_table_1 (dummy_col STRING)
STORED AS AVRO
TBLPROPERTIES (
  'avro.schema.literal'='{
    "type":"record",
    "name":"dummy_record_1",
    "namespace":"dummy_namespace",
    "fields":[
      {"name":"field1","type":["null","string"],"default":null},
      {"name":"field2","type":["null",{"type":"bytes","logicalType":"decimal","precision":10,"scale":3}],"default":null}
    ]}'
);

SHOW CREATE TABLE dummy_table_1;
CREATE EXTERNAL TABLE dummy_like_table_1 LIKE dummy_table_1;
SHOW CREATE TABLE dummy_like_table_1;

ALTER TABLE dummy_table_1 SET TBLPROPERTIES (
  'avro.schema.literal'='{
    "type":"record",
    "name":"dummy_record_1",
    "namespace":"dummy_namespace",
    "fields":[
      {"name":"field1","type":["null","string"],"default":null},
      {"name":"field2","type":["null",{"type":"bytes","logicalType":"decimal","precision":10,"scale":3}],"default":null},
      {"name":"field3","type":["null","string"],"default":null}
    ]}'
);

SHOW CREATE TABLE dummy_table_1;
CREATE EXTERNAL TABLE dummy_like_table_2 LIKE dummy_table_1;
SHOW CREATE TABLE dummy_like_table_2;

DROP TABLE dummy_like_table_1;
DROP TABLE dummy_like_table_2;
DROP TABLE dummy_table_1;