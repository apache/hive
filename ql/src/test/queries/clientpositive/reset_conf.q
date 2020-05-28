set hive.skewjoin.key;
set hive.skewjoin.mapjoin.min.split;
set hive.skewjoin.key=300000;
set hive.skewjoin.mapjoin.min.split=256000000;
set hive.skewjoin.key;
set hive.skewjoin.mapjoin.min.split;

reset;

set hive.skewjoin.key;
set hive.skewjoin.mapjoin.min.split;

set hive.skewjoin.key=300000;
set hive.skewjoin.mapjoin.min.split=256000000;
select 'After setting hive.skewjoin.key and hive.skewjoin.mapjoin.min.split';
set hive.skewjoin.key;

reset -d hive.skewjoin.key;
select 'After resetting hive.skewjoin.key to default';
set hive.skewjoin.key;
set hive.skewjoin.mapjoin.min.split;

set hive.skewjoin.key=300000;

reset -d hive.skewjoin.key hive.skewjoin.mapjoin.min.split;
select 'After resetting both to default';
set hive.skewjoin.key;
set hive.skewjoin.mapjoin.min.split;

