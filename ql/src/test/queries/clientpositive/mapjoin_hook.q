--! qt:dataset:srcpart
--! qt:dataset:src1
--! qt:dataset:src
set hive.stats.column.autogather=false;
set hive.exec.post.hooks = org.apache.hadoop.hive.ql.hooks.MapJoinCounterHook,org.apache.hadoop.hive.ql.hooks.PrintCompletedTasksHook;

drop table dest1_n171;
CREATE TABLE dest1_n171(key INT, value STRING) STORED AS TEXTFILE;
set hive.strict.checks.bucketing=false;

CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
CREATE TABLE tab_part (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
CREATE TABLE srcbucket_mapjoin_part (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket21.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket23.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');



set hive.optimize.bucketingsorting=false;
insert overwrite table tab_part partition (ds='2008-04-08')
  select key,value from srcbucket_mapjoin_part;

CREATE TABLE tab(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab partition (ds='2008-04-08')
  select key,value from srcbucket_mapjoin;

DROP TABLE IF EXISTS alltypesorc;

CREATE TABLE alltypesorc(
    ctinyint TINYINT,
    csmallint SMALLINT,
    cint INT,
    cbigint BIGINT,
    cfloat FLOAT,
    cdouble DOUBLE,
    cstring1 STRING,
    cstring2 STRING,
    ctimestamp1 TIMESTAMP,
    ctimestamp2 TIMESTAMP,
    cboolean1 BOOLEAN,
    cboolean2 BOOLEAN)
    STORED AS ORC;

LOAD DATA LOCAL INPATH "../../data/files/alltypesorc" OVERWRITE INTO  TABLE alltypesorc;

set hive.auto.convert.join = true;

INSERT OVERWRITE TABLE dest1_n171 
SELECT /*+ MAPJOIN(x) */ x.key, count(1) FROM src1 x JOIN src y ON (x.key = y.key) group by x.key;


FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key = src3.key)
INSERT OVERWRITE TABLE dest1_n171 SELECT src1.key, src3.value;


set hive.mapjoin.localtask.max.memory.usage = 0.0001;
set hive.mapjoin.check.memory.rows = 2;
set hive.auto.convert.join.noconditionaltask = false;


FROM srcpart src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1_n171 SELECT src1.key, src2.value 
where (src1.ds = '2008-04-08' or src1.ds = '2008-04-09' )and (src1.hr = '12' or src1.hr = '11');


FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key + src2.key = src3.key)
INSERT OVERWRITE TABLE dest1_n171 SELECT src1.key, src3.value;

set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;
set hive.convert.join.bucket.mapjoin.tez = true;

--Bucket Map Join
select count(*)
from
(select a.key as key, a.value as value from tab a join tab_part b on a.key = b.key) c
  join
  tab_part d on c.key = d.key;


set hive.optimize.dynamic.partition.hashjoin=true;
set hive.auto.convert.join.noconditionaltask.size=20000;
set hive.exec.reducers.bytes.per.reducer=20000;
set hive.stats.fetch.column.stats=false;

--Dynamic Partition Hash Join

select
*
from alltypesorc a join alltypesorc b on a.cint = b.cint
where
a.cint between 1000000 and 3000000 and b.cbigint is not null
order by a.cint;

set hive.auto.convert.join = false;

