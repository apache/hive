
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/sfs;
dfs -cp ${system:hive.root}/data/files/table1_delim.txt ${system:test.tmp.dir}/sfs/f1.txt;
dfs -cp ${system:hive.root}/data/files/table1_delim.txt ${system:test.tmp.dir}/sfs/f2.txt;

create external table t1 (a string,b string,c string) location 'sfs:${system:test.tmp.dir}/sfs';

desc formatted t1;

select count(1) from t1;
