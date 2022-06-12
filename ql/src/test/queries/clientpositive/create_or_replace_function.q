-- create dump files for testing
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/perm_fn.jar;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/new_perm_fn.jar;

create temporary function or replace temp_fn as 'org.apache.hadoop.hive.ql.udf.UDFAscii';

create function or replace perm_fn as 'org.apache.hadoop.hive.ql.udf.UDFAscii' using jar 'file:${system:test.tmp.dir}/perm_fn.jar';
-- replace function with another jar
create function or replace perm_fn as 'org.apache.hadoop.hive.ql.udf.UDFAscii' using jar 'file:${system:test.tmp.dir}/new_perm_fn.jar';

drop temporary function temp_fn;
drop function perm_fn;
