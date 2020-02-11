--! qt:dataset:src
--! qt:dataset:srcpart
-- Test explain diagnostics with Erasure Coding

ERASURE echo listPolicies originally was;
ERASURE listPolicies;

show table extended like srcpart;

desc formatted srcpart;

explain select key, value from srcpart;

explain extended select key, value from srcpart;

show table extended like src;

desc formatted src;

explain select key, value from src;

explain extended select key, value from src;


