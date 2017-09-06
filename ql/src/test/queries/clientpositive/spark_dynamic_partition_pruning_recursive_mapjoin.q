SET hive.spark.dynamic.partition.pruning=true;
SET hive.auto.convert.join=true;
SET hive.strict.checks.cartesian.product=false;

CREATE TABLE part_table1 (col int) PARTITIONED BY (part1_col int);
CREATE TABLE part_table2 (col int) PARTITIONED BY (part2_col int);
CREATE TABLE part_table3 (col int) PARTITIONED BY (part3_col int);
CREATE TABLE part_table4 (col int) PARTITIONED BY (part4_col int);
CREATE TABLE part_table5 (col int) PARTITIONED BY (part5_col int);

CREATE TABLE reg_table (col int);

ALTER TABLE part_table1 ADD PARTITION (part1_col = 1);

ALTER TABLE part_table2 ADD PARTITION (part2_col = 1);
ALTER TABLE part_table2 ADD PARTITION (part2_col = 2);

ALTER TABLE part_table3 ADD PARTITION (part3_col = 1);
ALTER TABLE part_table3 ADD PARTITION (part3_col = 2);
ALTER TABLE part_table3 ADD PARTITION (part3_col = 3);

ALTER TABLE part_table4 ADD PARTITION (part4_col = 1);
ALTER TABLE part_table4 ADD PARTITION (part4_col = 2);
ALTER TABLE part_table4 ADD PARTITION (part4_col = 3);
ALTER TABLE part_table4 ADD PARTITION (part4_col = 4);

ALTER TABLE part_table5 ADD PARTITION (part5_col = 1);
ALTER TABLE part_table5 ADD PARTITION (part5_col = 2);
ALTER TABLE part_table5 ADD PARTITION (part5_col = 3);
ALTER TABLE part_table5 ADD PARTITION (part5_col = 4);
ALTER TABLE part_table5 ADD PARTITION (part5_col = 5);

INSERT INTO TABLE part_table1 PARTITION (part1_col = 1) VALUES (1);

INSERT INTO TABLE part_table2 PARTITION (part2_col = 1) VALUES (1);
INSERT INTO TABLE part_table2 PARTITION (part2_col = 2) VALUES (2);

INSERT INTO TABLE part_table3 PARTITION (part3_col = 1) VALUES (1);
INSERT INTO TABLE part_table3 PARTITION (part3_col = 2) VALUES (2);
INSERT INTO TABLE part_table3 PARTITION (part3_col = 3) VALUES (3);

INSERT INTO TABLE part_table4 PARTITION (part4_col = 1) VALUES (1);
INSERT INTO TABLE part_table4 PARTITION (part4_col = 2) VALUES (2);
INSERT INTO TABLE part_table4 PARTITION (part4_col = 3) VALUES (3);
INSERT INTO TABLE part_table4 PARTITION (part4_col = 4) VALUES (4);

INSERT INTO TABLE part_table5 PARTITION (part5_col = 1) VALUES (1);
INSERT INTO TABLE part_table5 PARTITION (part5_col = 2) VALUES (2);
INSERT INTO TABLE part_table5 PARTITION (part5_col = 3) VALUES (3);
INSERT INTO TABLE part_table5 PARTITION (part5_col = 4) VALUES (4);
INSERT INTO TABLE part_table5 PARTITION (part5_col = 5) VALUES (5);

INSERT INTO table reg_table VALUES (1), (2), (3), (4), (5), (6);

-- 3 table join pt2 pruned based on scan from pt1
explain SELECT *
        FROM   part_table1 pt1,
               part_table2 pt2,
               reg_table rt
        WHERE  rt.col = pt1.part1_col
        AND    pt2.part2_col = pt1.part1_col;

SELECT *
FROM   part_table1 pt1,
       part_table2 pt2,
       reg_table rt
WHERE  rt.col = pt1.part1_col
AND    pt2.part2_col = pt1.part1_col;

-- 4 table join pt3 pruned based on pt2, pt2 pruned based on pt1
explain SELECT *
        FROM   part_table1 pt1,
               part_table2 pt2,
               part_table3 pt3,
               reg_table rt
        WHERE  rt.col = pt1.part1_col
        AND    pt2.part2_col = pt1.part1_col
        AND    pt3.part3_col = pt1.part1_col;

SELECT *
FROM   part_table1 pt1,
       part_table2 pt2,
       part_table3 pt3,
       reg_table rt
WHERE  rt.col = pt1.part1_col
AND    pt2.part2_col = pt1.part1_col
AND    pt3.part3_col = pt1.part1_col;

-- 5 table join pt4 pruned based on pt3, pt3 pruned based on pt2, pt2 pruned based on pt1
explain SELECT *
        FROM   part_table1 pt1,
               part_table2 pt2,
               part_table3 pt3,
               part_table4 pt4,
               reg_table rt
        WHERE  rt.col = pt1.part1_col
        AND    pt2.part2_col = pt1.part1_col
        AND    pt3.part3_col = pt1.part1_col
        AND    pt4.part4_col = pt1.part1_col;

SELECT *
FROM   part_table1 pt1,
       part_table2 pt2,
       part_table3 pt3,
       part_table4 pt4,
       reg_table rt
WHERE  rt.col = pt1.part1_col
AND    pt2.part2_col = pt1.part1_col
AND    pt3.part3_col = pt1.part1_col
AND    pt4.part4_col = pt1.part1_col;

-- 6 table join pt5 pruned based on pt4, pt4 pruned based on pt3, pt3 pruned based on pt2,
-- pt2 pruned based on pt1
explain SELECT *
        FROM   part_table1 pt1,
               part_table2 pt2,
               part_table3 pt3,
               part_table4 pt4,
               part_table5 pt5,
               reg_table rt
        WHERE  rt.col = pt1.part1_col
        AND    pt2.part2_col = pt1.part1_col
        AND    pt3.part3_col = pt1.part1_col
        AND    pt4.part4_col = pt1.part1_col
        AND    pt5.part5_col = pt1.part1_col;

SELECT *
FROM   part_table1 pt1,
       part_table2 pt2,
       part_table3 pt3,
       part_table4 pt4,
       part_table5 pt5,
       reg_table rt
WHERE  rt.col = pt1.part1_col
AND    pt2.part2_col = pt1.part1_col
AND    pt3.part3_col = pt1.part1_col
AND    pt4.part4_col = pt1.part1_col
AND    pt5.part5_col = pt1.part1_col;

-- Cleanup
DROP TABLE part_table1;
DROP TABLE part_table2;
DROP TABLE part_table3;
DROP TABLE part_table4;
DROP TABLE part_table5;

DROP TABLE reg_table;
