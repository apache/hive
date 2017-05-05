set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;

set hive.vectorized.adaptor.usage.mode=none;
set hive.vectorized.execution.enabled=true;
set hive.stats.fetch.column.stats=true;
set hive.tez.dynamic.semijoin.reduction.threshold=-999999999999;

-- Create Tables
create table dsrv2_big stored as orc as
  select
  cast(L_PARTKEY as bigint) as partkey_bigint,
  cast(L_PARTKEY as decimal(10,1)) as partkey_decimal,
  cast(L_PARTKEY as double) as partkey_double,
  cast(l_shipdate as date) as shipdate_date,
  cast(cast(l_shipdate as date) as timestamp) as shipdate_ts,
  cast(l_shipdate as string) as shipdate_string,
  cast(l_shipdate as char(10)) as shipdate_char,
  cast(l_shipdate as varchar(10)) as shipdate_varchar
  from lineitem;
create table dsrv2_small stored as orc as select * from dsrv2_big limit 20;
analyze table dsrv2_big compute statistics;
analyze table dsrv2_small compute statistics;
analyze table dsrv2_big compute statistics for columns;
analyze table dsrv2_small compute statistics for columns;

-- single key (bigint)
EXPLAIN select count(*) from dsrv2_big a join dsrv2_small b on (a.partkey_bigint = b.partkey_bigint);
select count(*) from dsrv2_big a join dsrv2_small b on (a.partkey_bigint = b.partkey_bigint);

-- single key (decimal)
EXPLAIN select count(*) from dsrv2_big a join dsrv2_small b on (a.partkey_decimal = b.partkey_decimal);
select count(*) from dsrv2_big a join dsrv2_small b on (a.partkey_decimal = b.partkey_decimal);

-- single key (double)
EXPLAIN select count(*) from dsrv2_big a join dsrv2_small b on (a.partkey_double = b.partkey_double);
select count(*) from dsrv2_big a join dsrv2_small b on (a.partkey_double = b.partkey_double);

-- single key (date)
EXPLAIN select count(*) from dsrv2_big a join dsrv2_small b on (a.shipdate_date = b.shipdate_date);
select count(*) from dsrv2_big a join dsrv2_small b on (a.shipdate_date = b.shipdate_date);

-- single key (timestamp)
EXPLAIN select count(*) from dsrv2_big a join dsrv2_small b on (a.shipdate_ts = b.shipdate_ts);
select count(*) from dsrv2_big a join dsrv2_small b on (a.shipdate_ts = b.shipdate_ts);

-- single key (string)
EXPLAIN select count(*) from dsrv2_big a join dsrv2_small b on (a.shipdate_string = b.shipdate_string);
select count(*) from dsrv2_big a join dsrv2_small b on (a.shipdate_string = b.shipdate_string);

-- single key (char)
EXPLAIN select count(*) from dsrv2_big a join dsrv2_small b on (a.shipdate_char = b.shipdate_char);
select count(*) from dsrv2_big a join dsrv2_small b on (a.shipdate_char = b.shipdate_char);

-- single key (varchar)
EXPLAIN select count(*) from dsrv2_big a join dsrv2_small b on (a.shipdate_varchar = b.shipdate_varchar);
select count(*) from dsrv2_big a join dsrv2_small b on (a.shipdate_varchar = b.shipdate_varchar);

drop table dsrv2_big;
drop table dsrv2_small;
