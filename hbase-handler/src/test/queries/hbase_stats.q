set datanucleus.cache.collections=false;

set hive.stats.dbclass=hbase;
analyze table src compute statistics;

desc extended src;

analyze table srcpart partition(ds='2008-04-08', hr=11) compute statistics;
analyze table srcpart partition(ds='2008-04-08', hr=12) compute statistics;

desc extended srcpart partition(ds='2008-04-08', hr=11);
desc extended srcpart partition(ds='2008-04-08', hr=12);
desc extended srcpart;

create table hbase_part like srcpart;

insert overwrite table hbase_part partition (ds='2010-04-08', hr = '11') select key, value from src;
insert overwrite table hbase_part partition (ds='2010-04-08', hr = '12') select key, value from src;

desc extended hbase_part;
desc extended hbase_part partition (ds='2010-04-08', hr = '11');
desc extended hbase_part partition (ds='2010-04-08', hr = '12');

