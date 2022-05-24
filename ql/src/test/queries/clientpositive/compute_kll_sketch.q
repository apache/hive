--! qt:dataset:src
--! qt:dataset:alltypesorc

select ds_kll_stringify(ds_kll_sketch(cast (ctinyint as float), 200)) from alltypesorc;
select ds_kll_stringify(ds_kll_sketch(cast (ctinyint as float))) from alltypesorc;

select ds_kll_stringify(ds_kll_sketch(cast (csmallint as float), 200)) from alltypesorc;
select ds_kll_stringify(ds_kll_sketch(cast (csmallint as float))) from alltypesorc;

select ds_kll_stringify(ds_kll_sketch(cast (cint as float), 200)) from alltypesorc;
select ds_kll_stringify(ds_kll_sketch(cast (cint as float))) from alltypesorc;

select ds_kll_stringify(ds_kll_sketch(cast (cbigint as float), 200)) from alltypesorc;
select ds_kll_stringify(ds_kll_sketch(cast (cbigint as float))) from alltypesorc;

select ds_kll_stringify(ds_kll_sketch(cfloat, 200)) from alltypesorc;
select ds_kll_stringify(ds_kll_sketch(cfloat)) from alltypesorc;

select ds_kll_stringify(ds_kll_sketch(cast (cdouble as float), 200)) from alltypesorc;
select ds_kll_stringify(ds_kll_sketch(cast (cdouble as float))) from alltypesorc;

create table test_compute_kll (key1 int, key2 int);
insert overwrite table test_compute_kll select a.key, b.key from src as a, src as b;

-- kll of two columns must be equal to each other
select ds_kll_stringify(ds_kll_sketch(cast (key1 as float), 200)) from test_compute_kll;
select ds_kll_stringify(ds_kll_sketch(cast (key2 as float))) from test_compute_kll;

select ds_kll_stringify(ds_kll_sketch(cast (key1 as float), 200)) from test_compute_kll;
select ds_kll_stringify(ds_kll_sketch(cast (key2 as float))) from test_compute_kll;
