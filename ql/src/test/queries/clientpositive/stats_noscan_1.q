--! qt:dataset:srcpart
set hive.explain.user=false;
set datanucleus.cache.collections=false;
set hive.stats.autogather=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- test analyze table ... compute statistics noscan

-- 1. test full spec
create table analyze_srcpart_n0 like srcpart;
insert overwrite table analyze_srcpart_n0 partition (ds, hr) select * from srcpart where ds is not null;
explain
analyze table analyze_srcpart_n0 PARTITION(ds='2008-04-08',hr=11) compute statistics noscan;
analyze table analyze_srcpart_n0 PARTITION(ds='2008-04-08',hr=11) compute statistics noscan;
analyze table analyze_srcpart_n0 PARTITION(ds='2008-04-08',hr=12) compute statistics noscan;
-- confirm result
describe formatted analyze_srcpart_n0 PARTITION(ds='2008-04-08',hr=11);
describe formatted analyze_srcpart_n0 PARTITION(ds='2008-04-08',hr=12);
describe formatted analyze_srcpart_n0 PARTITION(ds='2008-04-09',hr=11);
describe formatted analyze_srcpart_n0 PARTITION(ds='2008-04-09',hr=12);
describe formatted analyze_srcpart_n0;
drop table analyze_srcpart_n0;

-- 2. test partial spec
create table analyze_srcpart_partial like srcpart;
insert overwrite table analyze_srcpart_partial partition (ds, hr) select * from srcpart where ds is not null;
explain
analyze table analyze_srcpart_partial PARTITION(ds='2008-04-08') compute statistics noscan;
analyze table analyze_srcpart_partial PARTITION(ds='2008-04-08') compute statistics noscan;
-- confirm result
describe formatted analyze_srcpart_partial PARTITION(ds='2008-04-08',hr=11);
describe formatted analyze_srcpart_partial PARTITION(ds='2008-04-08',hr=12);
describe formatted analyze_srcpart_partial PARTITION(ds='2008-04-09',hr=11);
describe formatted analyze_srcpart_partial PARTITION(ds='2008-04-09',hr=12);
drop table analyze_srcpart_partial;



