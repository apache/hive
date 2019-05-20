set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
CREATE TABLE tab_part_n0 (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
CREATE TABLE t1_n5 (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE t1_n5 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE t1_n5 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE t1_n5 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000003_0' INTO TABLE t1_n5 partition(ds='2008-04-08');



set hive.optimize.bucketingsorting=false;
set hive.stats.dbclass=fs;

insert overwrite table tab_part_n0 partition (ds='2008-04-08')
select key,value from t1_n5;
describe formatted tab_part_n0 partition(ds='2008-04-08');
