-- only certain UDFs are allowed as default
create table t (i int, j string default repeat('s', 4));
