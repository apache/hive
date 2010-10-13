set datanucleus.cache.collections=false;

set hive.stats.dbclass=hbase;

create table stats_src like src;
insert overwrite table stats_src select * from src;
analyze table stats_src compute statistics;
desc formatted stats_src;

create table hbase_part like srcpart;

insert overwrite table hbase_part partition (ds='2010-04-08', hr = '11') select key, value from src;
insert overwrite table hbase_part partition (ds='2010-04-08', hr = '12') select key, value from src;

analyze table hbase_part partition(ds='2008-04-08', hr=11) compute statistics;
analyze table hbase_part partition(ds='2008-04-08', hr=12) compute statistics;

desc formatted hbase_part;
desc formatted hbase_part partition (ds='2010-04-08', hr = '11');
desc formatted hbase_part partition (ds='2010-04-08', hr = '12');

