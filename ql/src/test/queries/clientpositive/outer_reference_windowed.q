DROP TABLE IF EXISTS e011_01;
DROP TABLE IF EXISTS e011_02;
DROP TABLE IF EXISTS e011_03;

CREATE TABLE e011_01 (
  c1 decimal(15,2),
  c2 decimal(15,2))
  STORED AS TEXTFILE;

CREATE TABLE e011_02 (
  c1 decimal(15,2),
  c2 decimal(15,2));

CREATE TABLE e011_03 (
  c1 decimal(15,2),
  c2 decimal(15,2));

LOAD DATA
  LOCAL INPATH '../../data/files/e011_01.txt' 
  OVERWRITE 
  INTO TABLE e011_01;

INSERT INTO TABLE e011_02
  SELECT c1, c2 
  FROM e011_01;

INSERT INTO TABLE e011_03
  SELECT c1, c2 
  FROM e011_01;

ANALYZE TABLE e011_01 COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE e011_02 COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE e011_03 COMPUTE STATISTICS FOR COLUMNS;

set hive.explain.user=false;

describe formatted e011_01;

explain select sum(sum(c1)) over() from e011_01;
select sum(sum(c1)) over() from e011_01;

explain select sum(sum(c1)) over(
  partition by c2 order by c1)
  from e011_01
  group by e011_01.c1, e011_01.c2;
select sum(sum(c1)) over(
  partition by c2 order by c1)
  from e011_01
  group by e011_01.c1, e011_01.c2;

explain select sum(sum(e011_01.c1)) over(
  partition by e011_01.c2 order by e011_01.c1) 
  from e011_01 
  join e011_03 on e011_01.c1 = e011_03.c1
  group by e011_01.c1, e011_01.c2;
select sum(sum(e011_01.c1)) over(
  partition by e011_01.c2 order by e011_01.c1) 
  from e011_01 
  join e011_03 on e011_01.c1 = e011_03.c1
  group by e011_01.c1, e011_01.c2;

explain select sum(sum(e011_01.c1)) over(
  partition by e011_03.c2 order by e011_03.c1)
  from e011_01 
  join e011_03 on e011_01.c1 = e011_03.c1
  group by e011_03.c1, e011_03.c2;
select sum(sum(e011_01.c1)) over(
  partition by e011_03.c2 order by e011_03.c1)
  from e011_01 
  join e011_03 on e011_01.c1 = e011_03.c1
  group by e011_03.c1, e011_03.c2;

explain select sum(corr(e011_01.c1, e011_03.c1))
  over(partition by e011_01.c2 order by e011_03.c2)
  from e011_01
  join e011_03 on e011_01.c1 = e011_03.c1
  group by e011_03.c2, e011_01.c2;
select sum(corr(e011_01.c1, e011_03.c1))
  over(partition by e011_01.c2 order by e011_03.c2)
  from e011_01
  join e011_03 on e011_01.c1 = e011_03.c1
  group by e011_03.c2, e011_01.c2;
