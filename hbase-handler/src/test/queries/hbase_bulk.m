drop table hbsort;
drop table hbpartition;

-- this is a dummy table used for controlling how the HFiles are
-- created
create table hbsort(key string, val string, val2 string)
stored as
INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.hbase.HiveHFileOutputFormat'
TBLPROPERTIES ('hfile.family.path' = '/tmp/hbsort/cf');

-- this is a dummy table used for controlling how the input file
-- for TotalOrderPartitioner is created
create external table hbpartition(part_break string)
row format serde 
'org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe'
stored as 
inputformat 
'org.apache.hadoop.mapred.TextInputFormat'
outputformat 
'org.apache.hadoop.hive.ql.io.HiveNullValueSequenceFileOutputFormat'
location '/tmp/hbpartitions';

-- this should produce one file in /tmp/hbpartitions, but we do not
-- know what it will be called, so we will copy it to a well known
-- filename /tmp/hbpartition.lst
insert overwrite table hbpartition
select distinct value
from src
where value='val_100' or value='val_200';

dfs -count /tmp/hbpartitions;
dfs -cp /tmp/hbpartitions/* /tmp/hbpartition.lst;

set mapred.reduce.tasks=3;
set hive.mapred.partitioner=org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
set total.order.partitioner.natural.order=false;
set total.order.partitioner.path=/tmp/hbpartition.lst;

-- this should produce three files in /tmp/hbsort/cf
-- include some trailing blanks and nulls to make sure we handle them correctly
insert overwrite table hbsort
select distinct value,
  case when key=103 then cast(null as string) else key end,
  case when key=103 then ''
       else cast(key+1 as string) end
from src
cluster by value;

dfs -count /tmp/hbsort/cf;

-- To get the files out to your local filesystem for loading into
-- HBase, run mkdir -p /tmp/blah/cf, then uncomment and
-- semicolon-terminate the line below before running this test:
-- dfs -copyToLocal /tmp/hbsort/cf/* /tmp/blah/cf

drop table hbsort;
drop table hbpartition;
