SET hive.concatenate.external.table=true;
create external table test_ext_concat (i int) stored as orc;
alter table test_ext_concat concatenate;
drop table test_ext_concat;
