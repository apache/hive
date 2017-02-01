set hive.cli.print.header=true;
set hive.metastore.disallow.incompatible.col.type.changes=false;
set hive.optimize.ppd=false;
set hive.optimize.index.filter=false;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.map.aggr=false;
-- disabling map side aggregation as that can lead to different intermediate record counts



create table unique_1( 
i int, 
d string, 
s string) 
row format delimited 
fields terminated by '|' 
stored as textfile;

load data local inpath '../../data/files/unique_1.txt' into table unique_1;

create table test1 stored as orc as select * from unique_1 order by d;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;

alter table test1 change column d d double;

set hive.optimize.ppd=false;
set hive.optimize.index.filter=false;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

select s from test1 where d = -4996703.42;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

select s from test1 where d = -4996703.42;

set hive.optimize.ppd=true;
set hive.optimize.index.filter=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

select s from test1 where d = -4996703.42;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

select s from test1 where d = -4996703.42;
