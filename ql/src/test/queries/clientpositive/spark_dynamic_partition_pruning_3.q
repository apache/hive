set hive.stats.deserialization.factor=1.0;
CREATE TABLE partitioned_table1 (col int) PARTITIONED BY (part_col int);
CREATE TABLE partitioned_table2 (col int) PARTITIONED BY (part_col int);
CREATE TABLE partitioned_table3 (col int) PARTITIONED BY (part_col int);
CREATE TABLE partitioned_table4 (col int) PARTITIONED BY (part_col1 int, part_col2 int);
CREATE TABLE partitioned_table5 (col int) PARTITIONED BY (part_col1 int, part_col2 int);

CREATE TABLE regular_table1 (col1 int, col2 int);
CREATE TABLE regular_table2 (col1 int, col2 int);

ALTER TABLE partitioned_table1 ADD PARTITION (part_col = 1);
ALTER TABLE partitioned_table1 ADD PARTITION (part_col = 2);
ALTER TABLE partitioned_table1 ADD PARTITION (part_col = 3);

ALTER TABLE partitioned_table2 ADD PARTITION (part_col = 1);
ALTER TABLE partitioned_table2 ADD PARTITION (part_col = 2);
ALTER TABLE partitioned_table2 ADD PARTITION (part_col = 3);

ALTER TABLE partitioned_table3 ADD PARTITION (part_col = 1);
ALTER TABLE partitioned_table3 ADD PARTITION (part_col = 2);
ALTER TABLE partitioned_table3 ADD PARTITION (part_col = 3);

ALTER TABLE partitioned_table4 ADD PARTITION (part_col1 = 1, part_col2 = 1);
ALTER TABLE partitioned_table4 ADD PARTITION (part_col1 = 2, part_col2 = 2);
ALTER TABLE partitioned_table4 ADD PARTITION (part_col1 = 3, part_col2 = 3);

ALTER TABLE partitioned_table5 ADD PARTITION (part_col1 = 1, part_col2 = 1);
ALTER TABLE partitioned_table5 ADD PARTITION (part_col1 = 2, part_col2 = 2);
ALTER TABLE partitioned_table5 ADD PARTITION (part_col1 = 3, part_col2 = 3);

INSERT INTO TABLE regular_table1 VALUES (0, 0), (1, 1), (2, 2);
INSERT INTO TABLE regular_table2 VALUES (0, 0), (1, 1), (2, 2);

INSERT INTO TABLE partitioned_table1 PARTITION (part_col = 1) VALUES (1), (2), (3), (4), (5), (6);
INSERT INTO TABLE partitioned_table1 PARTITION (part_col = 2) VALUES (1), (2), (3), (4), (5), (6);
INSERT INTO TABLE partitioned_table1 PARTITION (part_col = 3) VALUES (1), (2), (3), (4), (5), (6);

INSERT INTO TABLE partitioned_table2 PARTITION (part_col = 1) VALUES (1), (2), (3), (4), (5), (6);
INSERT INTO TABLE partitioned_table2 PARTITION (part_col = 2) VALUES (1), (2), (3), (4), (5), (6);
INSERT INTO TABLE partitioned_table2 PARTITION (part_col = 3) VALUES (1), (2), (3), (4), (5), (6);

INSERT INTO TABLE partitioned_table3 PARTITION (part_col = 1) VALUES (1), (2), (3), (4), (5), (6);
INSERT INTO TABLE partitioned_table3 PARTITION (part_col = 2) VALUES (1), (2), (3), (4), (5), (6);
INSERT INTO TABLE partitioned_table3 PARTITION (part_col = 3) VALUES (1), (2), (3), (4), (5), (6);

INSERT INTO TABLE partitioned_table4 PARTITION (part_col1 = 1, part_col2 = 1) VALUES (1), (2), (3), (4), (5), (6);
INSERT INTO TABLE partitioned_table4 PARTITION (part_col1 = 2, part_col2 = 2) VALUES (1), (2), (3), (4), (5), (6);
INSERT INTO TABLE partitioned_table4 PARTITION (part_col1 = 3, part_col2 = 3) VALUES (1), (2), (3), (4), (5), (6);

INSERT INTO TABLE partitioned_table5 PARTITION (part_col1 = 1, part_col2 = 1) VALUES (1), (2), (3), (4), (5), (6);
INSERT INTO TABLE partitioned_table5 PARTITION (part_col1 = 2, part_col2 = 2) VALUES (1), (2), (3), (4), (5), (6);
INSERT INTO TABLE partitioned_table5 PARTITION (part_col1 = 3, part_col2 = 3) VALUES (1), (2), (3), (4), (5), (6);

SET hive.spark.dynamic.partition.pruning.map.join.only=true;
SET hive.strict.checks.cartesian.product=false;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask.size=15; -- ensure the partitioned tables are treated as big tables

-- left semi join where the subquery is a join between a partitioned and a non-partitioned table
EXPLAIN SELECT count(*) FROM partitioned_table1 WHERE partitioned_table1.part_col IN (
SELECT regular_table1.col1 FROM regular_table1 JOIN partitioned_table2 ON
regular_table1.col1 = partitioned_table2.part_col AND partitioned_table2.col > 3 AND regular_table1.col1 > 1);

SELECT count(*)
FROM   partitioned_table1
WHERE  partitioned_table1.part_col IN (SELECT regular_table1.col1
                                       FROM   regular_table1
                                              JOIN partitioned_table2
                                                ON
              regular_table1.col1 = partitioned_table2.part_col AND partitioned_table2.col > 3 AND regular_table1.col1 > 1);

-- three-table join with dpp against one partitioned table
EXPLAIN SELECT count(*) FROM partitioned_table1, regular_table1 rt1,
regular_table1 rt2 WHERE rt1.col1 = partitioned_table1.part_col AND rt2.col1 =
partitioned_table1.part_col AND rt1.col2 > 0 AND rt2.col2 > 1; 

SELECT count(*)
FROM   partitioned_table1,
       regular_table1 rt1,
       regular_table1 rt2
WHERE  rt1.col1 = partitioned_table1.part_col
       AND rt2.col1 = partitioned_table1.part_col
       AND rt1.col2 > 0
       AND rt2.col2 > 1;

-- four-table join with dpp against two separate partitioned tables
EXPLAIN SELECT count(*) FROM partitioned_table1, partitioned_table2, regular_table1 rt1 
, regular_table1 rt2 WHERE rt1.col1 = partitioned_table1.part_col AND rt2.col1 = 
partitioned_table2.part_col AND rt1.col2 > 0 AND rt2.col2 > 1; 

SELECT count(*)
FROM   partitioned_table1,
	   partitioned_table2,
       regular_table1 rt1,
       regular_table1 rt2
WHERE  rt1.col1 = partitioned_table1.part_col
       AND rt2.col1 = partitioned_table2.part_col
       AND rt1.col2 > 0
       AND rt2.col2 > 1;

-- dpp with between filter
EXPLAIN SELECT count(*) FROM regular_table1, partitioned_table1 WHERE regular_table1.col1
= partitioned_table1.part_col AND regular_table1.col2 BETWEEN 1 AND 3;

SELECT count(*)
FROM   regular_table1,
       partitioned_table1
WHERE  regular_table1.col1 = partitioned_table1.part_col
       AND regular_table1.col2 BETWEEN 1 AND 3;

-- dpp with cte
EXPLAIN WITH q1 AS (SELECT regular_table1.col1 AS col FROM regular_table1 WHERE
regular_table1.col2 > 1), q2 AS (SELECT partitioned_table1.part_col AS col FROM
partitioned_table1 WHERE partitioned_table1.col > 1) SELECT count(*) FROM q1 JOIN q2 ON
q1.col = q2.col;

WITH q1
     AS (SELECT regular_table1.col1 AS col
         FROM   regular_table1
         WHERE  regular_table1.col2 > 1),
     q2
     AS (SELECT partitioned_table1.part_col AS col
         FROM   partitioned_table1
         WHERE  partitioned_table1.col > 1)
SELECT count(*)
FROM   q1
       JOIN q2
         ON q1.col = q2.col;

-- join two partitioned tables with a filter
EXPLAIN SELECT count(*) FROM partitioned_table1, partitioned_table2 WHERE
partitioned_table1.part_col = partitioned_table2.part_col AND partitioned_table2.col > 1;

SELECT count(*)
FROM   partitioned_table1,
       partitioned_table2
WHERE  partitioned_table1.part_col = partitioned_table2.part_col
       AND partitioned_table2.col > 1;

-- dpp betwen two partitioned tables, both with multiple partition columns
SET hive.auto.convert.join.noconditionaltask.size=150; -- set auto convert size to a higher value so map-joins are triggered for the partitioned tables

EXPLAIN SELECT count(*) FROM partitioned_table4, partitioned_table5 WHERE
partitioned_table4.part_col1 = partitioned_table5.part_col1 AND
partitioned_table4.part_col2 = partitioned_table5.part_col2;

SELECT count(*)
FROM   partitioned_table4,
       partitioned_table5
WHERE  partitioned_table4.part_col1 = partitioned_table5.part_col1
       AND partitioned_table4.part_col2 = partitioned_table5.part_col2;

-- dpp is pushed through multiple levels of joins
EXPLAIN SELECT count(*) FROM partitioned_table1 JOIN regular_table1 ON
partitioned_table1.part_col = regular_table1.col1 JOIN regular_table2 ON
regular_table1.col1 = regular_table2.col1;

SELECT count(*)
FROM   partitioned_table1
       JOIN regular_table1
         ON partitioned_table1.part_col = regular_table1.col1
       JOIN regular_table2
         ON regular_table1.col1 = regular_table2.col1;

SET hive.auto.convert.join.noconditionaltask.size=15; -- reset auto convert size to previous value

-- three-way join where the partitioned table is the smallest table
-- disabled until HIVE-17225 is fixed
-- EXPLAIN SELECT * FROM partitioned_table1, regular_table1 rt1, regular_table2 rt2
-- WHERE rt1.col1 = partitioned_table1.part_col AND rt2.col1 =
-- partitioned_table1.part_col AND partitioned_table1.col > 3;

-- SELECT *
-- FROM   partitioned_table1,
--        regular_table1 rt1,
--        regular_table2 rt2
-- WHERE  rt1.col1 = partitioned_table1.part_col
--        AND rt2.col1 = partitioned_table1.part_col AND partitioned_table1.col > 3;

-- dpp is pushed to partition columns that are added to each other
-- disabled until HIVE-17244 is fixed
-- EXPLAIN SELECT count(*) FROM partitioned_table4 pt4 JOIN regular_table1 rt1
-- ON pt4.part_col1 + pt4.part_col2 = rt1.col1 + 1;

-- SELECT count(*)
-- FROM   partitioned_table4 pt4
--        JOIN regular_table1 rt1
--          ON pt4.part_col1 + pt4.part_col2 = rt1.col1 + 1;

-- dpp pushed to all union operands
-- disabled until HIVE-17239 is fixed
-- EXPLAIN SELECT count(*) FROM (SELECT part_col FROM partitioned_table1 UNION ALL SELECT
-- part_col FROM partitioned_table2) q1 JOIN regular_table1 JOIN regular_table2
-- WHERE q1.part_col = regular_table1.col1 AND q1.part_col = regular_table2.col1;

-- SELECT count(*)
-- FROM   (
--               SELECT part_col
--               FROM   partitioned_table1
--               UNION ALL
--               SELECT part_col
--               FROM   partitioned_table2) q1
-- JOIN   regular_table1
-- JOIN   regular_table2
-- where  q1.part_col = regular_table1.col1
-- AND    q1.part_col = regular_table2.col1;

-- left semi join where the subquery is a join between a partitioned and a partitioned table
-- disabled until HIVE-17238 is fixed
-- EXPLAIN SELECT count(*) FROM partitioned_table1 WHERE partitioned_table1.part_col IN (
-- SELECT partitioned_table2.col FROM partitioned_table2 JOIN partitioned_table3 ON
-- partitioned_table3.col = partitioned_table2.part_col AND partitioned_table2.col > 1);

-- SELECT count(*)
-- FROM   partitioned_table1
-- WHERE  partitioned_table1.part_col IN (SELECT partitioned_table2.col
--                                        FROM   partitioned_table2
--                                               JOIN partitioned_table3
--                                                 ON
--               partitioned_table3.col = partitioned_table2.part_col
--               AND partitioned_table2.col > 1);

DROP TABLE partitioned_table1;
DROP TABLE partitioned_table2;
DROP TABLE partitioned_table3;
DROP TABLE partitioned_table4;
DROP TABLE partitioned_table5;
DROP TABLE regular_table1;
DROP TABLE regular_table2;
