create table legacy_table (d date)
stored as orc;

load data local inpath '../../data/files/orc_legacy_mixed_dates.orc' into table legacy_table;

select * from legacy_table;

set orc.proleptic.gregorian.default=true;

select * from legacy_table;

drop table legacy_table;