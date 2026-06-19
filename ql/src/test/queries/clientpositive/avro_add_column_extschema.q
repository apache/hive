-- verify that we can modify avro tables created by externalschemas if we drop avro-related tblproperties

CREATE TABLE avro_extschema_literal
STORED AS AVRO
TBLPROPERTIES ('avro.schema.literal'='{
  "namespace": "org.apache.hive",
  "name": "ext_schema",
  "type": "record",
  "fields": [
    { "name":"number", "type":"int" },
    { "name":"first_name", "type":"string" },
    { "name":"last_name", "type":"string" }
  ] }');

DESCRIBE avro_extschema_literal;

ALTER TABLE avro_extschema_literal UNSET TBLPROPERTIES ('avro.schema.literal');

ALTER TABLE avro_extschema_literal
CHANGE COLUMN number number bigint;

DESCRIBE avro_extschema_literal;

ALTER TABLE avro_extschema_literal
ADD COLUMNS (age int);

DESCRIBE avro_extschema_literal;

dfs -cp ${system:hive.root}data/files/grad.avsc ${system:test.tmp.dir}/avro_add_column_extschema.avsc;


CREATE TABLE avro_extschema_url
STORED AS AVRO
TBLPROPERTIES ('avro.schema.url'='${system:test.tmp.dir}/avro_add_column_extschema.avsc');

DESCRIBE avro_extschema_url;

ALTER TABLE avro_extschema_url UNSET TBLPROPERTIES ('avro.schema.url');

ALTER TABLE avro_extschema_url
CHANGE COLUMN col6 col6 bigint;

DESCRIBE avro_extschema_url;

ALTER TABLE avro_extschema_url
ADD COLUMNS (col7 int);

DESCRIBE avro_extschema_url;

dfs -rm ${system:test.tmp.dir}/avro_add_column_extschema.avsc;
