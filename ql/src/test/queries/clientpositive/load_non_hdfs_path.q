dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/non_hdfs_path;
dfs -touchz ${system:test.tmp.dir}/non_hdfs_path/1.txt;
dfs -chmod 555 ${system:test.tmp.dir}/non_hdfs_path/1.txt;

create table t1_n129(i int);
load data inpath 'pfile:${system:test.tmp.dir}/non_hdfs_path/' overwrite into table t1_n129;
