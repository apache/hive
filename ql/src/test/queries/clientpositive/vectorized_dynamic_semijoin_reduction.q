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

-- Create Tables
create table dsrv_big stored as orc as select key as key_str, cast(key as int) as key_int, value from src;
create table dsrv_small stored as orc as select distinct key as key_str, cast(key as int) as key_int, value from src where key < 100;

-- single key (int)
EXPLAIN VECTORIZATION EXPRESSION select count(*) from dsrv_big a join dsrv_small b on (a.key_int = b.key_int);
select count(*) from dsrv_big a join dsrv_small b on (a.key_int = b.key_int);

-- single key (string)
EXPLAIN VECTORIZATION EXPRESSION select count(*) from dsrv_big a join dsrv_small b on (a.key_str = b.key_str);
select count(*) from dsrv_big a join dsrv_small b on (a.key_str = b.key_str);

-- keys are different type
EXPLAIN VECTORIZATION EXPRESSION select count(*) from dsrv_big a join dsrv_small b on (a.key_str = b.key_str);
select count(*) from dsrv_big a join dsrv_small b on (a.key_int = b.key_str);

-- multiple tables
EXPLAIN VECTORIZATION EXPRESSION select count(*) from dsrv_big a, dsrv_small b, dsrv_small c where a.key_int = b.key_int and a.key_int = c.key_int;
select count(*) from dsrv_big a, dsrv_small b, dsrv_small c where a.key_int = b.key_int and a.key_int = c.key_int;

-- multiple keys
EXPLAIN VECTORIZATION EXPRESSION select count(*) from dsrv_big a join dsrv_small b on (a.key_str = b.key_str and a.key_int = b.key_int);
select count(*) from dsrv_big a join dsrv_small b on (a.key_str = b.key_str and a.key_int = b.key_int);

-- small table result is empty
EXPLAIN VECTORIZATION EXPRESSION select count(*) from dsrv_big a join dsrv_small b on (a.key_int = b.key_int) where b.value in ('nonexistent1', 'nonexistent2');
select count(*) from dsrv_big a join dsrv_small b on (a.key_int = b.key_int) where b.value in ('nonexistent1', 'nonexistent2');

-- bloomfilter expectedEntries should use ndv if available. Compare to first query
analyze table dsrv_small compute statistics;
analyze table dsrv_small compute statistics for columns;
set hive.stats.fetch.column.stats=true;
EXPLAIN select count(*) from dsrv_big a join dsrv_small b on (a.key_int = b.key_int);

drop table dsrv_big;
drop table dsrv_small;
