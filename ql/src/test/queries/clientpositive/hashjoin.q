SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=10000000;
SET hive.cbo.enable=false;

CREATE TABLE t_hashjoin_big(
  cint int,
  cvarchar varchar(50),
  cdouble double,
  a int
);

CREATE TABLE t_hashjoin_small(
  cint int,
  cvarchar varchar(50),
  cdouble double
);

INSERT INTO t_hashjoin_big VALUES
(5, 'two', 3.0, 1),
(6, 'two', 1.5, 2),
(NULL, NULL, NULL, NULL),
(7, 'eight', 4.2, 3), (7, 'eight', 4.2, 4), (7, 'eight', 4.2, 5),
(5, 'one', 2.8, 6), (5, 'one', 2.8, 7), (5, 'one', 2.8, 8);

INSERT INTO t_hashjoin_small VALUES
(7, 'two', 1.5),
(5, 'two', 4.2),
(NULL, NULL, NULL),
(5, 'one', 1.1), (5, 'one', 1.1);

EXPLAIN
SELECT * FROM t_hashjoin_big x JOIN t_hashjoin_small z ON (x.cint = z.cint);
SELECT * FROM t_hashjoin_big x JOIN t_hashjoin_small z ON (x.cint = z.cint);


EXPLAIN
SELECT * FROM t_hashjoin_big x JOIN t_hashjoin_small z ON (x.cvarchar = z.cvarchar);
SELECT * FROM t_hashjoin_big x JOIN t_hashjoin_small z ON (x.cvarchar = z.cvarchar);


EXPLAIN
SELECT * FROM t_hashjoin_big x JOIN t_hashjoin_small z ON (x.cint = z.cint AND x.cvarchar = z.cvarchar);
SELECT * FROM t_hashjoin_big x JOIN t_hashjoin_small z ON (x.cint = z.cint AND x.cvarchar = z.cvarchar);

EXPLAIN
SELECT * FROM t_hashjoin_big x LEFT OUTER JOIN t_hashjoin_small z ON (x.cint = z.cint AND x.cvarchar = z.cvarchar);
SELECT * FROM t_hashjoin_big x LEFT OUTER JOIN t_hashjoin_small z ON (x.cint = z.cint AND x.cvarchar = z.cvarchar);


EXPLAIN
SELECT * FROM t_hashjoin_big x JOIN t_hashjoin_small z ON (x.cdouble = z.cdouble);
SELECT * FROM t_hashjoin_big x JOIN t_hashjoin_small z ON (x.cdouble = z.cdouble);

RESET hive.cbo.enable;

DROP TABLE t_hashjoin_big;
DROP TABLE t_hashjoin_small;