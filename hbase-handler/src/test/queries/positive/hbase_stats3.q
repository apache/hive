set datanucleus.cache.collections=false;
set hive.stats.autogather=true;
set hive.stats.atomic=false;
set hive.stats.collect.uncompressedsize=false;

create table stats_part like srcpart;

set hive.stats.key.prefix.max.length=0;

-- The stats key should be hashed since the max length is too small
insert overwrite table stats_part partition (ds='2010-04-08', hr = '13') select key, value from src;

desc formatted stats_part partition (ds='2010-04-08', hr = '13');

set hive.stats.key.prefix.max.length=200;

-- The stats key should not be hashed since the max length is large enough
insert overwrite table stats_part partition (ds='2010-04-08', hr = '13') select key, value from src;

desc formatted stats_part partition (ds='2010-04-08', hr = '13');

set hive.stats.key.prefix.max.length=-1;

-- The stats key should not be hashed since negative values should imply hashing is turned off
insert overwrite table stats_part partition (ds='2010-04-08', hr = '13') select key, value from src;

desc formatted stats_part partition (ds='2010-04-08', hr = '13');

set hive.stats.key.prefix.max.length=0;

-- Verify the stats are correct for dynamic partitions

-- The stats key should be hashed since the max length is too small
insert overwrite table stats_part partition (ds='2010-04-08', hr) select key, value, '13' from src;

desc formatted stats_part partition (ds='2010-04-08', hr = '13');

set hive.stats.key.prefix.max.length=200;

-- The stats key should not be hashed since the max length is large enough
insert overwrite table stats_part partition (ds='2010-04-08', hr) select key, value, '13' from src;

desc formatted stats_part partition (ds='2010-04-08', hr = '13');

set hive.stats.key.prefix.max.length=-1;

-- The stats key should not be hashed since negative values should imply hashing is turned off
insert overwrite table stats_part partition (ds='2010-04-08', hr) select key, value, '13' from src;

desc formatted stats_part partition (ds='2010-04-08', hr = '13');
