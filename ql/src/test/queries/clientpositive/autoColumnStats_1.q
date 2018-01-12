set hive.strict.checks.bucketing=false;

set hive.stats.column.autogather=true;
set hive.stats.fetch.column.stats=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.join.emit.interval=2;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;
set hive.auto.convert.sortmerge.join.bigtable.selection.policy = org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ;
set hive.optimize.bucketingsorting=false;

drop table src_multi1;

create table src_multi1 like src;

insert overwrite table src_multi1 select * from src;

explain extended select * from src_multi1;

describe formatted src_multi1;

drop table a;
drop table b;
create table a like src;
create table b like src;

from src
insert overwrite table a select *
insert overwrite table b select *;

describe formatted a;
describe formatted b;

drop table a;
drop table b;
create table a like src;
create table b like src;

from src
insert overwrite table a select *
insert into table b select *;

describe formatted a;
describe formatted b;


drop table src_multi2;

create table src_multi2 like src;

insert overwrite table src_multi2 select subq.key, src.value from (select * from src union select * from src1)subq join src on subq.key=src.key;

describe formatted src_multi2;


drop table nzhang_part14;

create table if not exists nzhang_part14 (key string)
  partitioned by (value string);

desc formatted nzhang_part14;

insert overwrite table nzhang_part14 partition(value) 
select key, value from (
  select * from (select 'k1' as key, cast(null as string) as value from src limit 2)a 
  union all
  select * from (select 'k2' as key, '' as value from src limit 2)b
  union all 
  select * from (select 'k3' as key, ' ' as value from src limit 2)c
) T;

desc formatted nzhang_part14 partition (value=' ');

explain select key from nzhang_part14;


drop table src5;

create table src5 as select key, value from src limit 5; 

insert overwrite table nzhang_part14 partition(value)
select key, value from src5; 

explain select key from nzhang_part14;


create table alter5 ( col1 string ) partitioned by (dt string);

alter table alter5 add partition (dt='a') location 'parta';

describe formatted alter5 partition (dt='a');

insert overwrite table alter5 partition (dt='a') select key from src ;

describe formatted alter5 partition (dt='a');

explain select * from alter5 where dt='a';


drop table src_stat_part;
create table src_stat_part(key string, value string) partitioned by (partitionId int);

insert overwrite table src_stat_part partition (partitionId=1)
select * from src1 limit 5;

describe formatted src_stat_part PARTITION(partitionId=1);

insert overwrite table src_stat_part partition (partitionId=2)
select * from src1;

describe formatted src_stat_part PARTITION(partitionId=2);

drop table srcbucket_mapjoin;
CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
drop table tab_part;
CREATE TABLE tab_part (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
drop table srcbucket_mapjoin_part;
CREATE TABLE srcbucket_mapjoin_part (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj1/000001_0' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');

insert overwrite table tab_part partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part;

describe formatted tab_part partition (ds='2008-04-08');

CREATE TABLE tab(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin;

describe formatted tab partition (ds='2008-04-08');

drop table nzhang_part14;

create table if not exists nzhang_part14 (key string, value string)
  partitioned by (ds string, hr string);

describe formatted nzhang_part14;

insert overwrite table nzhang_part14 partition(ds, hr) 
select key, value, ds, hr from (
  select * from (select 'k1' as key, cast(null as string) as value, '1' as ds, '2' as hr from src limit 2)a 
  union all
  select * from (select 'k2' as key, '' as value, '1' as ds, '3' as hr from src limit 2)b
  union all 
  select * from (select 'k3' as key, ' ' as value, '2' as ds, '1' as hr from src limit 2)c
) T;

desc formatted nzhang_part14 partition(ds='1', hr='3');


INSERT OVERWRITE TABLE nzhang_part14 PARTITION (ds='2010-03-03', hr)
SELECT key, value, hr FROM srcpart WHERE ds is not null and hr>10;

desc formatted nzhang_part14 PARTITION(ds='2010-03-03', hr='12');


drop table nzhang_part14;
create table if not exists nzhang_part14 (key string, value string)
partitioned by (ds string, hr string);

INSERT OVERWRITE TABLE nzhang_part14 PARTITION (ds='2010-03-03', hr)
SELECT key, value, hr FROM srcpart WHERE ds is not null and hr>10;

desc formatted nzhang_part14 PARTITION(ds='2010-03-03', hr='12');

drop table a;
create table a (key string, value string)
partitioned by (ds string, hr string);

drop table b;
create table b (key string, value string)
partitioned by (ds string, hr string);

drop table c;
create table c (key string, value string)
partitioned by (ds string, hr string);


FROM srcpart 
INSERT OVERWRITE TABLE a PARTITION (ds='2010-03-11', hr) SELECT key, value, hr WHERE ds is not null and hr>10
INSERT OVERWRITE TABLE b PARTITION (ds='2010-04-11', hr) SELECT key, value, hr WHERE ds is not null and hr>11
INSERT OVERWRITE TABLE c PARTITION (ds='2010-05-11', hr) SELECT key, value, hr WHERE hr>0;

explain select key from a;
explain select value from b;
explain select key from b;
explain select value from c;
explain select key from c;

