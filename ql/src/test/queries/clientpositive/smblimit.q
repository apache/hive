drop table if exists hlp1;
drop table if exists btl;
create table hlp1(c string);
load data local inpath '../../data/files/smbdata.txt' into table hlp1;
create table btl(c string) clustered by (c) sorted by (c) into 5 buckets;
insert overwrite table btl select * from hlp1;
SET hive.auto.convert.sortmerge.join = true;
SET hive.auto.convert.sortmerge.join.bigtable.selection.policy = org.apache.hadoop.hive.ql.optimizer.LeftmostBigTableSelectorForAutoSMJ;
SET hive.optimize.bucketmapjoin = true;
SET hive.optimize.bucketmapjoin.sortedmerge = true;
select 1 from btl join btl t1 on btl.c=t1.c limit 1;

