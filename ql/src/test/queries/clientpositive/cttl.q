set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

set metastore.create.as.acid=true;
set hive.default.fileformat=textfile;
set hive.default.fileformat.managed=orc;

dfs ${system:test.dfs.mkdir} hdfs:///tmp/hive19577_cttl;
dfs -copyFromLocal ../../data/files/students.txt hdfs:///tmp/hive19577_cttl/;
dfs -ls hdfs:///tmp/hive19577_cttl/;

drop table if exists students;
create external table students(
            name string,
            age int,
            gpa double)
  row format delimited
  fields terminated by '\t'
  stored as textfile
  location 'hdfs:///tmp/hive19577_cttl';

create temporary table temp1 like students;
insert into table temp1 select * from students;
select * from temp1 order by name, age limit 10; 

drop table students;
dfs -ls hdfs:///tmp/hive19577_cttl/;

dfs -rmr hdfs:///tmp/hive19577_cttl;
