-- reference to other column in default value is not allowed
create table t (i int, j double default cast(i as double));
