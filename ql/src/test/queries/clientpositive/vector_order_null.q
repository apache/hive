set hive.explain.user=false;
set hive.cli.print.header=true;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

create table src_null (a int, b string);
insert into src_null values (1, 'A');
insert into src_null values (null, null);
insert into src_null values (3, null);
insert into src_null values (2, null);
insert into src_null values (2, 'A');
insert into src_null values (2, 'B');

EXPLAIN VECTORIZATION DETAIL
SELECT x.* FROM src_null x ORDER BY a asc, b asc;
SELECT x.* FROM src_null x ORDER BY a asc, b asc;

EXPLAIN VECTORIZATION DETAIL
SELECT x.* FROM src_null x ORDER BY a desc, b asc;
SELECT x.* FROM src_null x ORDER BY a desc, b asc;

EXPLAIN VECTORIZATION DETAIL
SELECT x.* FROM src_null x ORDER BY b asc, a asc nulls last;
SELECT x.* FROM src_null x ORDER BY b asc, a asc nulls last;

EXPLAIN VECTORIZATION DETAIL
SELECT x.* FROM src_null x ORDER BY b desc, a asc;
SELECT x.* FROM src_null x ORDER BY b desc, a asc;

EXPLAIN VECTORIZATION DETAIL
SELECT x.* FROM src_null x ORDER BY a asc nulls first, b asc;
SELECT x.* FROM src_null x ORDER BY a asc nulls first, b asc;

EXPLAIN VECTORIZATION DETAIL
SELECT x.* FROM src_null x ORDER BY a desc nulls first, b asc;
SELECT x.* FROM src_null x ORDER BY a desc nulls first, b asc;

EXPLAIN VECTORIZATION DETAIL
SELECT x.* FROM src_null x ORDER BY b asc nulls last, a;
SELECT x.* FROM src_null x ORDER BY b asc nulls last, a;

EXPLAIN VECTORIZATION DETAIL
SELECT x.* FROM src_null x ORDER BY b desc nulls last, a;
SELECT x.* FROM src_null x ORDER BY b desc nulls last, a;

EXPLAIN VECTORIZATION DETAIL
SELECT x.* FROM src_null x ORDER BY a asc nulls last, b desc;
SELECT x.* FROM src_null x ORDER BY a asc nulls last, b desc;

EXPLAIN VECTORIZATION DETAIL
SELECT x.* FROM src_null x ORDER BY b desc nulls last, a desc nulls last;
SELECT x.* FROM src_null x ORDER BY b desc nulls last, a desc nulls last;

EXPLAIN VECTORIZATION DETAIL
SELECT x.* FROM src_null x ORDER BY b asc nulls first, a asc nulls last;
SELECT x.* FROM src_null x ORDER BY b asc nulls first, a asc nulls last;
