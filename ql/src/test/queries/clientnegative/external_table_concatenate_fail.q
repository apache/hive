create external table test_ext_concat (i int) stored as orc;
alter table test_ext_concat concatenate;
