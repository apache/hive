create table legacy_table (d timestamp)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':'
stored as avro;

load data local inpath '../../data/files/avro_legacy_mixed_timestamps.avro' into table legacy_table;

select * from legacy_table;

set hive.avro.timestamp.legacy.conversion.enabled=false;

select * from legacy_table;

set hive.avro.proleptic.gregorian.default=true;
set hive.avro.timestamp.legacy.conversion.enabled=true;

select * from legacy_table;

set hive.avro.timestamp.legacy.conversion.enabled=false;

select * from legacy_table;

drop table legacy_table;
