
dfs ${system:test.dfs.mkdir} -p file:///${system:test.tmp.dir}/sfs;
dfs -cp ${system:hive.root}/data/files/table1_delim.txt file:///${system:test.tmp.dir}/sfs/f1.txt;
dfs -cp ${system:hive.root}/data/files/table1_delim.txt file:///${system:test.tmp.dir}/sfs/f2.txt;

create external table t1 (a string,b string,c string) location 'file://${system:test.tmp.dir}/sfs';

select * from t1;

create external table t1s (a string,b string,c string) location 'sfs+file://${system:test.tmp.dir}/sfs/f1.txt/#SINGLEFILE#';

select * from t1s;

desc formatted t1s;

select count(1) from t1;
select count(1) from t1s;
