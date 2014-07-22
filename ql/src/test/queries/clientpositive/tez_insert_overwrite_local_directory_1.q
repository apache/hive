insert overwrite local directory '${system:test.tmp.dir}/tez_local_src_table_1'
select * from src order by key limit 10 ;
dfs -cat ${system:test.tmp.dir.uri}/tez_local_src_table_1/* ;

dfs -rmr ${system:test.tmp.dir.uri}/tez_local_src_table_1/ ;
