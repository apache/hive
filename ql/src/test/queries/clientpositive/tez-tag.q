--! qt:dataset:src1
SET hive.vectorized.execution.enabled=false;
set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
set hive.join.emit.interval=2;

set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.tez.dynamic.semijoin.reduction=false;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.stats.autogather=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;
set hive.stats.fetch.column.stats=true;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;
set hive.auto.convert.sortmerge.join.bigtable.selection.policy = org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ;

CREATE TABLE srcbucket_mapjoin_n9(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
CREATE TABLE tab_part_n6 (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
CREATE TABLE srcbucket_mapjoin_part_n9 (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

CREATE TABLE src2_n0 as select * from src1;
insert into src2_n0 select * from src2_n0;
insert into src2_n0 select * from src2_n0;

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_n9 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj1/000001_0' INTO TABLE srcbucket_mapjoin_n9 partition(ds='2008-04-08');

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_n9 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_n9 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part_n9 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part_n9 partition(ds='2008-04-08');

set hive.optimize.bucketingsorting=false;
insert overwrite table tab_part_n6 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part_n9;

CREATE TABLE tab_n5(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab_n5 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_n9;

set hive.convert.join.bucket.mapjoin.tez = true;
set hive.auto.convert.sortmerge.join = true;

set hive.auto.convert.join.noconditionaltask.size=0;
set hive.mapjoin.hybridgrace.minwbsize=125;
set hive.mapjoin.hybridgrace.minnumpartitions=4;

set hive.llap.memory.oversubscription.max.executors.per.query=3;

CREATE TABLE tab2_n2 (key int, value string, ds string);

insert into tab2_n2 select key, value, ds from tab_n5;
analyze table tab2_n2 compute statistics;
analyze table tab2_n2 compute statistics for columns;


explain select count(*) from tab_n5 a join tab_part_n6 b on a.key = b.key join src1 c on a.value = c.value;

select count(*) from tab_n5 a join tab_part_n6 b on a.key = b.key join src1 c on a.value = c.value;


explain select count(*) from (select x.key as key, min(x.value) as value from tab2_n2 x group by x.key) a join (select x.key as key, min(x.value) as value from tab2_n2 x group by x.key) b on a.key = b.key join src1 c on a.value = c.value where c.key < 0;
