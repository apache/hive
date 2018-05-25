set datanucleus.cache.collections=false;
set hive.stats.autogather=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

create table analyze_srcpart_n1 like srcpart;
insert overwrite table analyze_srcpart_n1 partition (ds, hr) select * from srcpart where ds is not null;

explain analyze table analyze_srcpart_n1 PARTITION(ds='2008-04-08',hr=11) compute statistics;
analyze table analyze_srcpart_n1 PARTITION(ds='2008-04-08',hr=11) compute statistics;
describe formatted analyze_srcpart_n1 PARTITION(ds='2008-04-08',hr=11);
describe formatted analyze_srcpart_n1;

explain analyze table analyze_srcpart_n1 PARTITION(ds='2008-04-08',hr=12) compute statistics;
analyze table analyze_srcpart_n1 PARTITION(ds='2008-04-08',hr=12) compute statistics;
describe formatted analyze_srcpart_n1 PARTITION(ds='2008-04-08',hr=12);

explain analyze table analyze_srcpart_n1 PARTITION(ds='2008-04-09',hr=11) compute statistics;
analyze table analyze_srcpart_n1 PARTITION(ds='2008-04-09',hr=11) compute statistics;
describe formatted analyze_srcpart_n1 PARTITION(ds='2008-04-09',hr=11);

explain analyze table analyze_srcpart_n1 PARTITION(ds='2008-04-09',hr=12) compute statistics;
analyze table analyze_srcpart_n1 PARTITION(ds='2008-04-09',hr=12) compute statistics;
describe formatted analyze_srcpart_n1 PARTITION(ds='2008-04-09',hr=12);

explain analyze table analyze_srcpart_n1 PARTITION(ds, hr) compute statistics;
analyze table analyze_srcpart_n1 PARTITION(ds, hr) compute statistics;

describe formatted analyze_srcpart_n1 PARTITION(ds='2008-04-08',hr=11);
describe formatted analyze_srcpart_n1 PARTITION(ds='2008-04-08',hr=12);
describe formatted analyze_srcpart_n1 PARTITION(ds='2008-04-09',hr=11);
describe formatted analyze_srcpart_n1 PARTITION(ds='2008-04-09',hr=12);
describe formatted analyze_srcpart_n1;
