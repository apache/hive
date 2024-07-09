--! qt:dataset:src
--! qt:dataset:alltypesorc

explain prepare pint from select sum(ctinyint)/count(ctinyint) as ag from alltypesorc where cint = ? group by cint;
explain cbo prepare pint from select sum(ctinyint)/count(ctinyint) as ag from alltypesorc where cint = ? group by cint;
prepare pint from select sum(ctinyint)/count(ctinyint) as ag from alltypesorc where cint = ? group by cint;

execute pint using 1000828;

