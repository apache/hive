--! qt:dataset:src
--! qt:dataset:alltypesorc

-- check that both call (aggregation column alone, aggregation column + sketch size)
-- work with vectorization and that the computed values coincide
set hive.vectorized.execution.enabled=true;
select ds_kll_stringify(ds_kll_sketch(cast (ctinyint as float), 200)) from alltypesorc;
select ds_kll_stringify(ds_kll_sketch(cast (ctinyint as float))) from alltypesorc;
-- compare it against the non-vectorized execution
set hive.vectorized.execution.enabled=false;
select ds_kll_stringify(ds_kll_sketch(cast (ctinyint as float), 200)) from alltypesorc;
select ds_kll_stringify(ds_kll_sketch(cast (ctinyint as float))) from alltypesorc;

-- change the k parameter (data sketch size) for KLL to see if it's actually used
set hive.vectorized.execution.enabled=true;
select ds_kll_stringify(ds_kll_sketch(cast (ctinyint as float), 300)) from alltypesorc;
set hive.vectorized.execution.enabled=false;
select ds_kll_stringify(ds_kll_sketch(cast (ctinyint as float), 300)) from alltypesorc;

-- START: series of tests covering different data types
set hive.vectorized.execution.enabled=true;
select ds_kll_stringify(ds_kll_sketch(cast (csmallint as float), 200)) from alltypesorc;
select ds_kll_stringify(ds_kll_sketch(cast (csmallint as float))) from alltypesorc;
set hive.vectorized.execution.enabled=false;
select ds_kll_stringify(ds_kll_sketch(cast (csmallint as float), 200)) from alltypesorc;
select ds_kll_stringify(ds_kll_sketch(cast (csmallint as float))) from alltypesorc;

set hive.vectorized.execution.enabled=true;
select ds_kll_stringify(ds_kll_sketch(cast (cint as float), 200)) from alltypesorc;
select ds_kll_stringify(ds_kll_sketch(cast (cint as float))) from alltypesorc;
set hive.vectorized.execution.enabled=false;
select ds_kll_stringify(ds_kll_sketch(cast (cint as float), 200)) from alltypesorc;
select ds_kll_stringify(ds_kll_sketch(cast (cint as float))) from alltypesorc;

set hive.vectorized.execution.enabled=true;
select ds_kll_stringify(ds_kll_sketch(cast (cbigint as float), 200)) from alltypesorc;
select ds_kll_stringify(ds_kll_sketch(cast (cbigint as float))) from alltypesorc;
set hive.vectorized.execution.enabled=false;
select ds_kll_stringify(ds_kll_sketch(cast (cbigint as float), 200)) from alltypesorc;
select ds_kll_stringify(ds_kll_sketch(cast (cbigint as float))) from alltypesorc;

set hive.vectorized.execution.enabled=true;
select ds_kll_stringify(ds_kll_sketch(cfloat, 200)) from alltypesorc;
select ds_kll_stringify(ds_kll_sketch(cfloat)) from alltypesorc;
set hive.vectorized.execution.enabled=false;
select ds_kll_stringify(ds_kll_sketch(cfloat, 200)) from alltypesorc;
select ds_kll_stringify(ds_kll_sketch(cfloat)) from alltypesorc;

set hive.vectorized.execution.enabled=true;
select ds_kll_stringify(ds_kll_sketch(cast (cdouble as float), 200)) from alltypesorc;
select ds_kll_stringify(ds_kll_sketch(cast (cdouble as float))) from alltypesorc;
set hive.vectorized.execution.enabled=false;
select ds_kll_stringify(ds_kll_sketch(cast (cdouble as float), 200)) from alltypesorc;
select ds_kll_stringify(ds_kll_sketch(cast (cdouble as float))) from alltypesorc;
-- END: series of tests covering different data types

-- testing that the KLL sketch of two identical columns is equal
create table test_compute_kll (key1 int, key2 int);
insert overwrite table test_compute_kll select a.key, b.key from src as a, src as b;

set hive.vectorized.execution.enabled=true;
select ds_kll_stringify(ds_kll_sketch(cast (key1 as float))) from test_compute_kll;
select ds_kll_stringify(ds_kll_sketch(cast (key2 as float))) from test_compute_kll;

set hive.vectorized.execution.enabled=false;
select ds_kll_stringify(ds_kll_sketch(cast (key1 as float))) from test_compute_kll;
select ds_kll_stringify(ds_kll_sketch(cast (key2 as float))) from test_compute_kll;
