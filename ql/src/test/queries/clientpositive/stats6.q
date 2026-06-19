--! qt:dataset:srcpart
set datanucleus.cache.collections=false;
set hive.stats.autogather=false;
set hive.exec.dynamic.partition=true;

create table analyze_srcpart_n2 like srcpart;
insert overwrite table analyze_srcpart_n2 partition (ds, hr) select * from srcpart where ds is not null;

analyze table analyze_srcpart_n2 PARTITION(ds='2008-04-08',hr=11) compute statistics;
analyze table analyze_srcpart_n2 PARTITION(ds='2008-04-08',hr=12) compute statistics;

describe formatted analyze_srcpart_n2 PARTITION(ds='2008-04-08',hr=11);
describe formatted analyze_srcpart_n2 PARTITION(ds='2008-04-08',hr=12);
describe formatted analyze_srcpart_n2 PARTITION(ds='2008-04-09',hr=11);
describe formatted analyze_srcpart_n2 PARTITION(ds='2008-04-09',hr=12);

describe formatted analyze_srcpart_n2;
