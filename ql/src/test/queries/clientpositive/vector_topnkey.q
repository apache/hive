set hive.mapred.mode=nonstrict;
set hive.vectorized.execution.enabled=true;
set hive.optimize.topnkey=true;

set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.tez.min.bloom.filter.entries=1;

set hive.tez.dynamic.partition.pruning=true;
set hive.stats.fetch.column.stats=true;
set hive.cbo.enable=true;

CREATE TABLE t_test(
  cint1 int,
  cint2 int,
  cdouble double,
  cvarchar varchar(50),
  cdecimal1 decimal(10,2),
  cdecimal2 decimal(38,5)
);

INSERT INTO t_test VALUES
(NULL, NULL, NULL, NULL, NULL, NULL),
(8, 9, 2.0, 'one', 2.0, 2.0), (8, 9, 2.0, 'one', 2.0, 2.0),
(4, 2, 3.3, 'two', 3.3, 3.3),
(NULL, NULL, NULL, NULL, NULL, NULL),
(NULL, NULL, NULL, NULL, NULL, NULL),
(6, 2, 1.8, 'three', 1.8, 1.8),
(7, 8, 4.5, 'four', 4.5, 4.5), (7, 8, 4.5, 'four', 4.5, 4.5), (7, 8, 4.5, 'four', 4.5, 4.5),
(4, 1, 2.0, 'five', 2.0, 2.0), (4, 1, 2.0, 'five', 2.0, 2.0), (4, 1, 2.0, 'five', 2.0, 2.0),
(NULL, NULL, NULL, NULL, NULL, NULL);

EXPLAIN VECTORIZATION DETAIL
SELECT cint1 FROM t_test GROUP BY cint1 ORDER BY cint1 LIMIT 3;

SELECT cint1 FROM t_test GROUP BY cint1 ORDER BY cint1 LIMIT 3;
SELECT cint1, cint2 FROM t_test GROUP BY cint1, cint2 ORDER BY cint1, cint2 LIMIT 3;
SELECT cint1, cint2 FROM t_test GROUP BY cint1, cint2 ORDER BY cint1 DESC, cint2 LIMIT 3;
SELECT cint1, cdouble FROM t_test GROUP BY cint1, cdouble ORDER BY cint1, cdouble LIMIT 3;
SELECT cvarchar, cdouble FROM t_test GROUP BY cvarchar, cdouble ORDER BY cvarchar, cdouble LIMIT 3;
SELECT cdecimal1, cdecimal2 FROM t_test GROUP BY cdecimal1, cdecimal2 ORDER BY cdecimal1, cdecimal2 LIMIT 3;

DROP TABLE t_test;
