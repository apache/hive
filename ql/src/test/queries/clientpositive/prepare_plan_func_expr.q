--! qt:dataset:src
--! qt:dataset:alltypesorc
--SORT_QUERY_RESULTS

explain prepare pint from select sum(ctinyint)/count(ctinyint) as ag from alltypesorc where cint <= ?  and cbigint <= ? and cfloat != ? group by cint;

prepare pint from select sum(ctinyint)/count(ctinyint) as ag from alltypesorc where cint <= ?  and cbigint <= ? and cfloat != ? group by cint;

explain execute pint using 100, 5000000, 0.023;

execute pint using 100, 5000000, 0.023;

