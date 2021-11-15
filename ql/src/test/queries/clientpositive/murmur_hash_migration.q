--! qt:dataset:src
set hive.stats.column.autogather=false;
set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=30000;

CREATE TABLE srcbucket_mapjoin_n18_stage(key int, value string) partitioned by (ds string) STORED AS TEXTFILE TBLPROPERTIES("bucketing_version" = '1');
CREATE TABLE srcbucket_mapjoin_part_n20_stage (key int, value string) partitioned by (ds string) STORED AS TEXTFILE TBLPROPERTIES("bucketing_version" = '1');

CREATE TABLE srcbucket_mapjoin_n18(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE TBLPROPERTIES("bucketing_version" = '1');
CREATE TABLE srcbucket_mapjoin_part_n20 (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE TBLPROPERTIES("bucketing_version" = '1');

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_n18_stage partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj1/000001_0' INTO TABLE srcbucket_mapjoin_n18_stage partition(ds='2008-04-08');

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_n20_stage partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_n20_stage partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part_n20_stage partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part_n20_stage partition(ds='2008-04-08');

set hive.optimize.bucketingsorting=false;


insert overwrite table srcbucket_mapjoin_n18  partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_n18_stage limit 150;

insert overwrite table srcbucket_mapjoin_part_n20 partition (ds='2008-04-08')
  select key,value from srcbucket_mapjoin_part_n20_stage limit 150;

analyze table srcbucket_mapjoin_n18 compute statistics for columns;
analyze table srcbucket_mapjoin_part_n20 compute statistics for columns;


CREATE TABLE tab_part_n11 (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
explain extended
insert overwrite table tab_part_n11 partition (ds='2008-04-08')
  select key,value from srcbucket_mapjoin_part_n20;
insert overwrite table tab_part_n11 partition (ds='2008-04-08')
  select key,value from srcbucket_mapjoin_part_n20;

CREATE TABLE tab_n10(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
explain extended
insert overwrite table tab_n10 partition (ds='2008-04-08')
  select key,value from srcbucket_mapjoin_n18;
insert overwrite table tab_n10 partition (ds='2008-04-08')
  select key,value from srcbucket_mapjoin_n18;

analyze table tab_part_n11 compute statistics for columns;
analyze table tab_n10 compute statistics for columns;

explain extended
select t1.key, t1.value, t2.key, t2.value from srcbucket_mapjoin_n18 t1, srcbucket_mapjoin_part_n20 t2 where t1.key = t2.key order by t1.key, t1.value, t2.key, t2.value;
select t1.key, t1.value, t2.key, t2.value from srcbucket_mapjoin_n18 t1, srcbucket_mapjoin_part_n20 t2 where t1.key = t2.key order by t1.key, t1.value, t2.key, t2.value;

set hive.auto.convert.join=true;

explain extended
select t1.key, t1.value, t2.key, t2.value from tab_part_n11 t1, tab_n10 t2 where t1.key = t2.key order by t1.key, t1.value, t2.key, t2.value;
select t1.key, t1.value, t2.key, t2.value from tab_part_n11 t1, tab_n10 t2 where t1.key = t2.key order by t1.key, t1.value, t2.key, t2.value;


