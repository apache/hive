set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.join.emit.interval=100000;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.vectorized.execution.enabled=true;

-- SORT_QUERY_RESULTS

CREATE TABLE srcbucket_mapjoin_n21(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
CREATE TABLE tab_part_n13 (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS ORCFILE;
CREATE TABLE srcbucket_mapjoin_part_n22 (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_n21 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj1/000001_0' INTO TABLE srcbucket_mapjoin_n21 partition(ds='2008-04-08');

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_n22 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_n22 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part_n22 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part_n22 partition(ds='2008-04-08');



set hive.optimize.bucketingsorting=false;
insert overwrite table tab_part_n13 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part_n22;

analyze table tab_part_n13 partition (ds='2008-04-08') compute statistics for columns;

CREATE TABLE tab_n14(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS ORCFILE;
insert overwrite table tab_n14 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_n21;

analyze table tab_n14 partition (ds='2008-04-08') compute statistics for columns;

set hive.join.emit.interval=2;

set mapred.reduce.tasks=3;

select * from
(select * from tab_n14 where tab_n14.key = 0)a
full outer join
(select * from tab_part_n13 where tab_part_n13.key = 98)b join tab_part_n13 c on a.key = b.key and b.key = c.key;

