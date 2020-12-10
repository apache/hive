--! qt:dataset:src
--! qt:dataset:alltypesorc

select hex(compute_bit_vector(ctinyint, 'hll')) from alltypesorc;
select hex(compute_bit_vector(csmallint, 'hll')) from alltypesorc;
select hex(compute_bit_vector(cint, 'hll')) from alltypesorc;
select hex(compute_bit_vector(cast (cbigint as decimal), 'hll')) from alltypesorc;
select hex(compute_bit_vector(cfloat, 'hll')) from alltypesorc;
select hex(compute_bit_vector(cdouble, 'hll')) from alltypesorc;
select hex(compute_bit_vector(cstring1, 'hll')) from alltypesorc;
select hex(compute_bit_vector(cstring2, 'hll')) from alltypesorc;
select hex(compute_bit_vector(ctimestamp1, 'hll')) from alltypesorc;
select hex(compute_bit_vector(cast (ctimestamp2 as date), 'hll')) from alltypesorc;

create table test_compute_bit_vector (val1 string, val2 string);
insert overwrite table test_compute_bit_vector select a.value, b.value from src as a, src as b;

-- hll of two columns must be equal to each other
select hex(compute_bit_vector(val1, 'hll')) from test_compute_bit_vector;
select hex(compute_bit_vector(val2, 'hll')) from test_compute_bit_vector;