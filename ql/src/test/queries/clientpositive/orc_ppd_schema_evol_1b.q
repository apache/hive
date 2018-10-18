set hive.vectorized.execution.enabled=false;
set hive.cli.print.header=true;
set hive.metastore.disallow.incompatible.col.type.changes=true;
set hive.optimize.ppd=false;
set hive.optimize.index.filter=false;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.map.aggr=false;
-- disabling map side aggregation as that can lead to different intermediate record counts

create table unique_1_n0( 
i int, 
d double, 
s string) 
row format delimited 
fields terminated by '|' 
stored as textfile;

load data local inpath '../../data/files/unique_1.txt' into table unique_1_n0;

create table unique_2( 
i int, 
d double, 
s string)
row format delimited 
fields terminated by '|' 
stored as textfile;

load data local inpath '../../data/files/unique_2.txt' into table unique_2;

create table test_two_files( 
i int, 
d double, 
s string)
stored as orc;

insert into table test_two_files select * from unique_1_n0 where i <= 0;
insert into table test_two_files select * from unique_2 where i > 0;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;

alter table test_two_files change column i i string;

set hive.optimize.ppd=false;
set hive.optimize.index.filter=false;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

select s from test_two_files where i = -1591211872;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

select s from test_two_files where i = -1591211872;


set hive.optimize.ppd=true;
set hive.optimize.index.filter=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

select s from test_two_files where i = '-1591211872';

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

select s from test_two_files where i = -1591211872;

