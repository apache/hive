-- default for complex types are not allowed
create table t (i int, j array<default> default array(1.3, 2.3));
