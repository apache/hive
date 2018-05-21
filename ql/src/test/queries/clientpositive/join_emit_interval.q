set hive.strict.checks.cartesian.product=false;
set hive.join.emit.interval=1;

CREATE TABLE test1_n7 (key INT, value INT, col_1 STRING);
INSERT INTO test1_n7 VALUES (NULL, NULL, 'None'), (98, NULL, 'None'),
    (99, 0, 'Alice'), (99, 2, 'Mat'), (100, 1, 'Bob'), (101, 2, 'Car');

CREATE TABLE test2_n4 (key INT, value INT, col_2 STRING);
INSERT INTO test2_n4 VALUES (102, 2, 'Del'), (103, 2, 'Ema'),
    (104, 3, 'Fli'), (105, NULL, 'None');


-- Equi-condition and condition on one input (left outer join)
EXPLAIN
SELECT *
FROM test1_n7 LEFT OUTER JOIN test2_n4
ON (test1_n7.value=test2_n4.value AND test1_n7.key between 100 and 102);

SELECT *
FROM test1_n7 LEFT OUTER JOIN test2_n4
ON (test1_n7.value=test2_n4.value AND test1_n7.key between 100 and 102);

-- Condition on one input (left outer join)
EXPLAIN
SELECT *
FROM test1_n7 LEFT OUTER JOIN test2_n4
ON (test1_n7.key between 100 and 102);

SELECT *
FROM test1_n7 LEFT OUTER JOIN test2_n4
ON (test1_n7.key between 100 and 102);
