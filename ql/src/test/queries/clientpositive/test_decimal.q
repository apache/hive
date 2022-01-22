dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/test_sp_chars_decimal;
dfs -copyFromLocal ../../data/files/test_sp_chars_decimal.csv ${system:test.tmp.dir}/test_sp_chars_decimal/;
create external table test_sp_chars_decimal (id int, value decimal) ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ',' location '${system:test.tmp.dir}/test_sp_chars_decimal';
select * from test_sp_chars_decimal;
select distinct(value) from test_sp_chars_decimal;
