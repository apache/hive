create table src_null_n1 (a int, b string);
insert into src_null_n1 values (1, 'A');
insert into src_null_n1 values (null, null);
insert into src_null_n1 values (3, null);
insert into src_null_n1 values (2, null);
insert into src_null_n1 values (2, 'A');
insert into src_null_n1 values (2, 'B');

SELECT x.* FROM src_null_n1 x ORDER BY a asc;

SELECT x.* FROM src_null_n1 x ORDER BY a desc;

SELECT x.* FROM src_null_n1 x ORDER BY b asc, a asc nulls last;

SELECT x.* FROM src_null_n1 x ORDER BY b desc, a asc;

SELECT x.* FROM src_null_n1 x ORDER BY a asc nulls first;

SELECT x.* FROM src_null_n1 x ORDER BY a desc nulls first;

SELECT x.* FROM src_null_n1 x ORDER BY b asc nulls last, a;

SELECT x.* FROM src_null_n1 x ORDER BY b desc nulls last, a;

SELECT x.* FROM src_null_n1 x ORDER BY a asc nulls last, b desc;

SELECT x.* FROM src_null_n1 x ORDER BY b desc nulls last, a desc nulls last;

SELECT x.* FROM src_null_n1 x ORDER BY b asc nulls first, a asc nulls last;
