--! qt:dataset:srcpart
set datanucleus.cache.collections=false;
set hive.stats.autogather=false;
set hive.exec.dynamic.partition=true;

create table analyze_srcpart_n3 like srcpart;
insert overwrite table analyze_srcpart_n3 partition (ds, hr) select * from srcpart where ds is not null;

explain extended
analyze table analyze_srcpart_n3 PARTITION(ds='2008-04-08',hr) compute statistics;

analyze table analyze_srcpart_n3 PARTITION(ds='2008-04-08',hr) compute statistics;

desc formatted analyze_srcpart_n3;
desc formatted analyze_srcpart_n3 partition (ds='2008-04-08', hr=11);
desc formatted analyze_srcpart_n3 partition (ds='2008-04-08', hr=12);
desc formatted analyze_srcpart_n3 partition (ds='2008-04-09', hr=11);
desc formatted analyze_srcpart_n3 partition (ds='2008-04-09', hr=12);

