set hive.cli.print.header=true;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.ptf.enabled=true;
set hive.fetch.task.conversion=none;

DROP TABLE IF EXISTS e011_01_n0;
DROP TABLE IF EXISTS e011_02_n0;
DROP TABLE IF EXISTS e011_03_n0;

CREATE TABLE e011_01_n0 (
  c1 decimal(15,2),
  c2 decimal(15,2))
  STORED AS TEXTFILE;

CREATE TABLE e011_02_n0 (
  c1 decimal(15,2),
  c2 decimal(15,2));

CREATE TABLE e011_03_n0 (
  c1 decimal(15,2),
  c2 decimal(15,2));

CREATE TABLE e011_01_small (
  c1 decimal(7,2),
  c2 decimal(7,2))
  STORED AS TEXTFILE;

CREATE TABLE e011_02_small (
  c1 decimal(7,2),
  c2 decimal(7,2));

CREATE TABLE e011_03_small (
  c1 decimal(7,2),
  c2 decimal(7,2));

LOAD DATA
  LOCAL INPATH '../../data/files/e011_01.txt'
  OVERWRITE
  INTO TABLE e011_01_n0;

INSERT INTO TABLE e011_02_n0
  SELECT c1, c2
  FROM e011_01_n0;

INSERT INTO TABLE e011_03_n0
  SELECT c1, c2
  FROM e011_01_n0;

LOAD DATA
  LOCAL INPATH '../../data/files/e011_01.txt'
  OVERWRITE
  INTO TABLE e011_01_small;

INSERT INTO TABLE e011_02_small
  SELECT c1, c2
  FROM e011_01_small;

INSERT INTO TABLE e011_03_small
  SELECT c1, c2
  FROM e011_01_small;

ANALYZE TABLE e011_01_n0 COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE e011_02_n0 COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE e011_03_n0 COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE e011_01_small COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE e011_02_small COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE e011_03_small COMPUTE STATISTICS FOR COLUMNS;

set hive.explain.user=false;

explain vectorization detail
select sum(sum(c1)) over() from e011_01_n0;
select sum(sum(c1)) over() from e011_01_n0;

explain vectorization detail
select sum(sum(c1)) over(
  partition by c2 order by c1)
  from e011_01_n0
  group by e011_01_n0.c1, e011_01_n0.c2;
select sum(sum(c1)) over(
  partition by c2 order by c1)
  from e011_01_n0
  group by e011_01_n0.c1, e011_01_n0.c2;

explain vectorization detail
select sum(sum(e011_01_n0.c1)) over(
  partition by e011_01_n0.c2 order by e011_01_n0.c1) 
  from e011_01_n0 
  join e011_03_n0 on e011_01_n0.c1 = e011_03_n0.c1
  group by e011_01_n0.c1, e011_01_n0.c2;
select sum(sum(e011_01_n0.c1)) over(
  partition by e011_01_n0.c2 order by e011_01_n0.c1) 
  from e011_01_n0 
  join e011_03_n0 on e011_01_n0.c1 = e011_03_n0.c1
  group by e011_01_n0.c1, e011_01_n0.c2;

explain vectorization detail
select sum(sum(e011_01_n0.c1)) over(
  partition by e011_03_n0.c2 order by e011_03_n0.c1)
  from e011_01_n0 
  join e011_03_n0 on e011_01_n0.c1 = e011_03_n0.c1
  group by e011_03_n0.c1, e011_03_n0.c2;
select sum(sum(e011_01_n0.c1)) over(
  partition by e011_03_n0.c2 order by e011_03_n0.c1)
  from e011_01_n0 
  join e011_03_n0 on e011_01_n0.c1 = e011_03_n0.c1
  group by e011_03_n0.c1, e011_03_n0.c2;

explain vectorization detail
select sum(corr(e011_01_n0.c1, e011_03_n0.c1))
  over(partition by e011_01_n0.c2 order by e011_03_n0.c2)
  from e011_01_n0
  join e011_03_n0 on e011_01_n0.c1 = e011_03_n0.c1
  group by e011_03_n0.c2, e011_01_n0.c2;
select sum(corr(e011_01_n0.c1, e011_03_n0.c1))
  over(partition by e011_01_n0.c2 order by e011_03_n0.c2)
  from e011_01_n0
  join e011_03_n0 on e011_01_n0.c1 = e011_03_n0.c1
  group by e011_03_n0.c2, e011_01_n0.c2;



explain vectorization detail
select sum(sum(c1)) over() from e011_01_small;
select sum(sum(c1)) over() from e011_01_small;

explain vectorization detail
select sum(sum(c1)) over(
  partition by c2 order by c1)
  from e011_01_small
  group by e011_01_small.c1, e011_01_small.c2;
select sum(sum(c1)) over(
  partition by c2 order by c1)
  from e011_01_small
  group by e011_01_small.c1, e011_01_small.c2;

explain vectorization detail
select sum(sum(e011_01_small.c1)) over(
  partition by e011_01_small.c2 order by e011_01_small.c1)
  from e011_01_small 
  join e011_03_small on e011_01_small.c1 = e011_03_small.c1
  group by e011_01_small.c1, e011_01_small.c2;
select sum(sum(e011_01_small.c1)) over(
  partition by e011_01_small.c2 order by e011_01_small.c1)
  from e011_01_small
  join e011_03_small on e011_01_small.c1 = e011_03_small.c1
  group by e011_01_small.c1, e011_01_small.c2;

explain vectorization detail
select sum(sum(e011_01_small.c1)) over(
  partition by e011_03_small.c2 order by e011_03_small.c1)
  from e011_01_small
  join e011_03_small on e011_01_small.c1 = e011_03_small.c1
  group by e011_03_small.c1, e011_03_small.c2;
select sum(sum(e011_01_small.c1)) over(
  partition by e011_03_small.c2 order by e011_03_small.c1)
  from e011_01_small
  join e011_03_small on e011_01_small.c1 = e011_03_small.c1
  group by e011_03_small.c1, e011_03_small.c2;

explain vectorization detail
select sum(corr(e011_01_small.c1, e011_03_small.c1))
  over(partition by e011_01_small.c2 order by e011_03_small.c2)
  from e011_01_small
  join e011_03_small on e011_01_small.c1 = e011_03_small.c1
  group by e011_03_small.c2, e011_01_small.c2;
select sum(corr(e011_01_small.c1, e011_03_small.c1))
  over(partition by e011_01_small.c2 order by e011_03_small.c2)
  from e011_01_small
  join e011_03_small on e011_01_small.c1 = e011_03_small.c1
  group by e011_03_small.c2, e011_01_small.c2;
