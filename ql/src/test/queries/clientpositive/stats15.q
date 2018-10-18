--! qt:dataset:srcpart
--! qt:dataset:src
set datanucleus.cache.collections=false;

create table stats_src_n0 like src;
insert overwrite table stats_src_n0 select * from src;
analyze table stats_src_n0 compute statistics;
desc formatted stats_src_n0;

create table stats_part_n0 like srcpart;

insert overwrite table stats_part_n0 partition (ds='2010-04-08', hr = '11') select key, value from src;
insert overwrite table stats_part_n0 partition (ds='2010-04-08', hr = '12') select key, value from src;

analyze table stats_part_n0 partition(ds='2010-04-08', hr='11') compute statistics;
analyze table stats_part_n0 partition(ds='2010-04-08', hr='12') compute statistics;

insert overwrite table stats_part_n0 partition (ds='2010-04-08', hr = '13') select key, value from src;

desc formatted stats_part_n0;
desc formatted stats_part_n0 partition (ds='2010-04-08', hr = '11');
desc formatted stats_part_n0 partition (ds='2010-04-08', hr = '12');

analyze table stats_part_n0 partition(ds, hr) compute statistics;
desc formatted stats_part_n0;

drop table stats_src_n0;
drop table stats_part_n0;
