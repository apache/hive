--! qt:timezone:Asia/Singapore

create table legacy_table_avro1 (date_test timestamp)
stored as avro;

load data local inpath '../../data/files/tbl_avro1/' into table legacy_table_avro1;

select * from legacy_table_avro1;

set hive.avro.timestamp.legacy.conversion.enabled=false;

select * from legacy_table_avro1;

set hive.avro.timestamp.legacy.conversion.enabled=true;
set hive.vectorized.execution.enabled=false;

select * from legacy_table_avro1;

set hive.avro.timestamp.legacy.conversion.enabled=false;

select * from legacy_table_avro1;

drop table legacy_table_avro1;
