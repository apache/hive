--! qt:dataset:src
--! qt:dataset:alltypesorc

select hex(compute_bit_vector_hll(ctinyint)) from alltypesorc;
select hex(compute_bit_vector_hll(csmallint)) from alltypesorc;
select hex(compute_bit_vector_hll(cint)) from alltypesorc;
select hex(compute_bit_vector_hll(cast (cbigint as decimal))) from alltypesorc;
select hex(compute_bit_vector_hll(cfloat)) from alltypesorc;
select hex(compute_bit_vector_hll(cdouble)) from alltypesorc;
select hex(compute_bit_vector_hll(cstring1)) from alltypesorc;
select hex(compute_bit_vector_hll(cstring2)) from alltypesorc;
select hex(compute_bit_vector_hll(ctimestamp1)) from alltypesorc;
select hex(compute_bit_vector_hll(cast (ctimestamp2 as date))) from alltypesorc;

create table test_compute_bit_vector (val1 string, val2 string);
insert overwrite table test_compute_bit_vector select a.value, b.value from src as a, src as b;

-- hll of two columns must be equal to each other
select hex(compute_bit_vector_hll(val1)) from test_compute_bit_vector;
select hex(compute_bit_vector_hll(val2)) from test_compute_bit_vector;