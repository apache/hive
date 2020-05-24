set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;

SET hive.llap.io.enabled=true;
set hive.llap.cache.allow.synthetic.fileid=true;

create table if not exists alltypes (
 bo1 boolean,
 ti1 tinyint,
 si1 smallint,
 i1 int,
 bi1 bigint,
 f1 float,
 d1 double,
 de1 decimal,
 ts1 timestamp,
 da1 timestamp,
 s1 string,
 vc1 varchar(5),
 m1 map<string, string>,
 l1 array<int>,
 st1 struct<c1:int, c2:string>
) row format delimited fields terminated by '|'
collection items terminated by ','
map keys terminated by ':' stored as textfile;

load data local inpath '../../data/files/alltypes.txt' overwrite into table alltypes;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;
-- disables llap io for all etl (should not see LLAP IO COUNTERS)
set hive.llap.io.etl.skip.format=all;
create table alltypes_orc stored as orc as select * from alltypes;
insert into alltypes_orc select * from alltypes;

-- disables llap io for all etl + text (should not see LLAP IO COUNTERS)
set hive.llap.io.etl.skip.format=encode;
create table alltypes_text1 stored as textfile as select * from alltypes;
insert into alltypes_text1 select * from alltypes;

-- does not disable llap io for etl (should see LLAP IO COUNTERS)
set hive.llap.io.etl.skip.format=none;
create table alltypes_text2 stored as textfile as select * from alltypes;
insert into alltypes_text2 select * from alltypes;

drop table alltypes_text1;
drop table alltypes_text2;
drop table alltypes_orc;
