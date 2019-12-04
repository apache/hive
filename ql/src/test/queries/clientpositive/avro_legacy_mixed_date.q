create table legacy_table (d date)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':'
stored as avro;

load data local inpath '../../data/files/avro_legacy_mixed_dates.avro' into table legacy_table;

select * from legacy_table;

set hive.avro.proleptic.gregorian.default=true;

select * from legacy_table;

drop table legacy_table;
