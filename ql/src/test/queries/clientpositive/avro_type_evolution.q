-- File Schema { "name" : "val", "type" : [ "null", "int" ] }
-- Record Schema { "name" : "val", "type" : [ "long", "null" ] }

DROP TABLE IF EXISTS avro_type_evolution;

CREATE TABLE avro_type_evolution (val bigint) STORED AS AVRO
TBLPROPERTIES (
    'avro.schema.literal'='{
  "type" : "record",
  "name" : "type_evolution",
  "namespace" : "default",
  "fields" : [ {
    "name" : "val",
    "type" : [ "long", "null" ]
  } ]
}');
LOAD DATA LOCAL INPATH '../../data/files/type_evolution.avro' OVERWRITE INTO TABLE avro_type_evolution;
SELECT * FROM avro_type_evolution;

DROP TABLE avro_type_evolution;
