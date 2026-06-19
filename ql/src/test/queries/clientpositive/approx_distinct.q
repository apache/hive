

create temporary table random_types_table (
cboolean boolean,
cint int,
cbigint bigint,
cfloat float,
cdouble double,
cdecimal64 decimal(7,2),
cdecimal128 decimal(38,18),
cdate date,
ctimestamp timestamp,
cstring string,
cvarchar varchar(3),
cchar char(3)
) stored as orc;

select 'expect 1', approx_distinct(1);
select 'expect 1', approx_distinct(1.0);
select 'expect 1', approx_distinct(false); 
select 'expect 1', approx_distinct('X');
select 'expect 1', approx_distinct(current_date);
select 'expect 1', approx_distinct(current_timestamp);
select 'expect 1', approx_distinct(1.0BD);
select 'expect 1', approx_distinct(INTERVAL '1' DAY);

-- No rows (all 0)

select 'expect 0', approx_distinct(cboolean) from random_types_table;
select 'expect 0', approx_distinct(cint) from random_types_table;
select 'expect 0', approx_distinct(cbigint) from random_types_table;
select 'expect 0', approx_distinct(cfloat) from random_types_table;
select 'expect 0', approx_distinct(cdouble) from random_types_table;
select 'expect 0', approx_distinct(cdecimal64) from random_types_table;
select 'expect 0', approx_distinct(cdecimal128) from random_types_table;
select 'expect 0', approx_distinct(cdate) from random_types_table;
select 'expect 0', approx_distinct(ctimestamp) from random_types_table;
select 'expect 0', approx_distinct(cstring) from random_types_table;
select 'expect 0', approx_distinct(cvarchar) from random_types_table;
select 'expect 0', approx_distinct(cchar) from random_types_table;

-- 1 row twice (all 1)

insert into random_types_table values (true, 1, 1, 1.0, 1.0, 1.0BD, 1.0BD, '2000-01-01', '2000-01-01 00:00:01', 'A', 'B', 'C'); 
insert into random_types_table values (true, 1, 1, 1.0, 1.0, 1.0BD, 1.0BD, '2000-01-01', '2000-01-01 00:00:01', 'A', 'B', 'C'); 

select 'expect 1', approx_distinct(cboolean) from random_types_table;
select 'expect 1', approx_distinct(cint) from random_types_table;
select 'expect 1', approx_distinct(cbigint) from random_types_table;
select 'expect 1', approx_distinct(cfloat) from random_types_table;
select 'expect 1', approx_distinct(cdouble) from random_types_table;
select 'expect 1', approx_distinct(cdecimal64) from random_types_table;
select 'expect 1', approx_distinct(cdecimal128) from random_types_table;
select 'expect 1', approx_distinct(cdate) from random_types_table;
select 'expect 1', approx_distinct(ctimestamp) from random_types_table;
select 'expect 1', approx_distinct(cstring) from random_types_table;
select 'expect 1', approx_distinct(cvarchar) from random_types_table;
select 'expect 1', approx_distinct(cchar) from random_types_table;



insert into random_types_table values (false, 2, 2, 2.0, 2.0, 2.0BD, 2.0BD, '1999-12-31', '1999-12-31 00:00:01', 'X', 'Y', 'Z'); 

-- 2 unique rows (all 2)
select 'expect 2', approx_distinct(cboolean) from random_types_table;
select 'expect 2', approx_distinct(cint) from random_types_table;
select 'expect 2', approx_distinct(cbigint) from random_types_table;
select 'expect 2', approx_distinct(cfloat) from random_types_table;
select 'expect 2', approx_distinct(cdouble) from random_types_table;
select 'expect 2', approx_distinct(cdecimal64) from random_types_table;
select 'expect 2', approx_distinct(cdecimal128) from random_types_table;
select 'expect 2', approx_distinct(cdate) from random_types_table;
select 'expect 2', approx_distinct(ctimestamp) from random_types_table;
select 'expect 2', approx_distinct(cstring) from random_types_table;
select 'expect 2', approx_distinct(cvarchar) from random_types_table;
select 'expect 2', approx_distinct(cchar) from random_types_table;
