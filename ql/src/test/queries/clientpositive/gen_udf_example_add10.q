add jar ${system:maven.local.repository}/org/apache/hive/hive-contrib/${system:hive.version}/hive-contrib-${system:hive.version}.jar;

create temporary function example_add10 as 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFAdd10';

create table t1_n102(x int,y double);
load data local inpath '../../data/files/T1.txt' into table t1_n102;

explain select example_add10(x) as a,example_add10(y) as b from t1_n102 order by a desc,b limit 10;

select example_add10(x) as a,example_add10(y) as b from t1_n102 order by a desc,b limit 10;

drop table t1_n102;
drop temporary function example_add10;
