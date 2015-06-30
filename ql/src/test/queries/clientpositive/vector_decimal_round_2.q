set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=minimal;

create table decimal_tbl_1_orc (dec decimal(38,18)) 
STORED AS ORC;

insert into table decimal_tbl_1_orc values(55555);

select * from decimal_tbl_1_orc;

-- EXPLAIN
-- SELECT dec, round(null), round(null, 0), round(125, null), 
-- round(1.0/0.0, 0), round(power(-1.0,0.5), 0)
-- FROM decimal_tbl_1_orc ORDER BY dec;

-- SELECT dec, round(null), round(null, 0), round(125, null), 
-- round(1.0/0.0, 0), round(power(-1.0,0.5), 0)
-- FROM decimal_tbl_1_orc ORDER BY dec;

EXPLAIN
SELECT
  round(dec) as d, round(dec, 0), round(dec, 1), round(dec, 2), round(dec, 3),
  round(dec, -1), round(dec, -2), round(dec, -3), round(dec, -4),
  round(dec, -5), round(dec, -6), round(dec, -7), round(dec, -8)
FROM decimal_tbl_1_orc ORDER BY d;

SELECT
  round(dec) as d, round(dec, 0), round(dec, 1), round(dec, 2), round(dec, 3),
  round(dec, -1), round(dec, -2), round(dec, -3), round(dec, -4),
  round(dec, -5), round(dec, -6), round(dec, -7), round(dec, -8)
FROM decimal_tbl_1_orc ORDER BY d;

create table decimal_tbl_2_orc (pos decimal(38,18), neg decimal(38,18)) 
STORED AS ORC;

insert into table decimal_tbl_2_orc values(125.315, -125.315);

select * from decimal_tbl_2_orc;

EXPLAIN
SELECT
  round(pos) as p, round(pos, 0),
  round(pos, 1), round(pos, 2), round(pos, 3), round(pos, 4),
  round(pos, -1), round(pos, -2), round(pos, -3), round(pos, -4),
  round(neg), round(neg, 0),
  round(neg, 1), round(neg, 2), round(neg, 3), round(neg, 4),
  round(neg, -1), round(neg, -2), round(neg, -3), round(neg, -4)
FROM decimal_tbl_2_orc ORDER BY p;

SELECT
  round(pos) as p, round(pos, 0),
  round(pos, 1), round(pos, 2), round(pos, 3), round(pos, 4),
  round(pos, -1), round(pos, -2), round(pos, -3), round(pos, -4),
  round(neg), round(neg, 0),
  round(neg, 1), round(neg, 2), round(neg, 3), round(neg, 4),
  round(neg, -1), round(neg, -2), round(neg, -3), round(neg, -4)
FROM decimal_tbl_2_orc ORDER BY p;

create table decimal_tbl_3_orc (dec decimal(38,18)) 
STORED AS ORC;

insert into table decimal_tbl_3_orc values(3.141592653589793);

select * from decimal_tbl_3_orc;

EXPLAIN
SELECT
  round(dec, -15) as d, round(dec, -16),
  round(dec, -13), round(dec, -14),
  round(dec, -11), round(dec, -12),
  round(dec, -9), round(dec, -10),
  round(dec, -7), round(dec, -8),
  round(dec, -5), round(dec, -6),
  round(dec, -3), round(dec, -4),
  round(dec, -1), round(dec, -2),
  round(dec, 0), round(dec, 1),
  round(dec, 2), round(dec, 3),
  round(dec, 4), round(dec, 5),
  round(dec, 6), round(dec, 7),
  round(dec, 8), round(dec, 9),
  round(dec, 10), round(dec, 11),
  round(dec, 12), round(dec, 13),
  round(dec, 13), round(dec, 14),
  round(dec, 15), round(dec, 16)
FROM decimal_tbl_3_orc ORDER BY d;

SELECT
  round(dec, -15) as d, round(dec, -16),
  round(dec, -13), round(dec, -14),
  round(dec, -11), round(dec, -12),
  round(dec, -9), round(dec, -10),
  round(dec, -7), round(dec, -8),
  round(dec, -5), round(dec, -6),
  round(dec, -3), round(dec, -4),
  round(dec, -1), round(dec, -2),
  round(dec, 0), round(dec, 1),
  round(dec, 2), round(dec, 3),
  round(dec, 4), round(dec, 5),
  round(dec, 6), round(dec, 7),
  round(dec, 8), round(dec, 9),
  round(dec, 10), round(dec, 11),
  round(dec, 12), round(dec, 13),
  round(dec, 13), round(dec, 14),
  round(dec, 15), round(dec, 16)
FROM decimal_tbl_3_orc ORDER BY d;

create table decimal_tbl_4_orc (pos decimal(38,18), neg decimal(38,18)) 
STORED AS ORC;

insert into table decimal_tbl_4_orc values(1809242.3151111344, -1809242.3151111344);

select * from decimal_tbl_4_orc;

EXPLAIN
SELECT round(pos, 9) as p, round(neg, 9), round(1809242.3151111344BD, 9), round(-1809242.3151111344BD, 9)
FROM decimal_tbl_4_orc ORDER BY p;

SELECT round(pos, 9) as p, round(neg, 9), round(1809242.3151111344BD, 9), round(-1809242.3151111344BD, 9)
FROM decimal_tbl_4_orc ORDER BY p;
