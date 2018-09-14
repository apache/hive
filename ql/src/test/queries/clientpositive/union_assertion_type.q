CREATE TABLE union_table_test_n0 (column1 string not null, column2 string not null, column3 string not null);
CREATE TABLE union_table_test_n1 (column1 string, column2 string, column3 string);
INSERT INTO union_table_test_n0 VALUES ('1', '2', '3'), ('4', '5', '6'), ('7', '8', '9'), ('10', '11', '12');
INSERT INTO union_table_test_n1 VALUES ('1', '2', '3'), ('4', '5', '6'), ('7', '8', '9'), ('10', '11', '12');

EXPLAIN
SELECT column1, x.column2, x.column3 FROM (
SELECT column1, column2, column3 FROM union_table_test_n0
UNION ALL
SELECT column1, column2, '5' as column3 FROM union_table_test_n1) x
WHERE x.column3 < '5';

SELECT column1, x.column2, x.column3 FROM (
SELECT column1, column2, column3 FROM union_table_test_n0
UNION ALL
SELECT column1, column2, '5' as column3 FROM union_table_test_n1) x
WHERE x.column3 < '5';

EXPLAIN
SELECT column1, x.column2, x.column3 FROM (
SELECT column1, column2, '5' as column3 FROM union_table_test_n1
UNION ALL
SELECT column1, column2, '5' as column3 FROM union_table_test_n0) x
WHERE x.column3 < '5';

SELECT column1, x.column2, x.column3 FROM (
SELECT column1, column2, '5' as column3 FROM union_table_test_n1
UNION ALL
SELECT column1, column2, '5' as column3 FROM union_table_test_n0) x
WHERE x.column3 < '5';

DROP TABLE union_table_test_n0;
DROP TABLE union_table_test_n1;

CREATE TABLE union_table_test_n3 (k int);
CREATE TABLE union_table_test_n4 (k int);
CREATE TABLE union_table_test_n5 (k int);
INSERT INTO union_table_test_n3 VALUES (1),(3);
INSERT INTO union_table_test_n4 VALUES (1);
INSERT INTO union_table_test_n5 VALUES (1),(3);

EXPLAIN
SELECT u0.k as key, u0.d1 as data0, u0.d2 as data2 FROM (
  SELECT k,'' as d1,'' as d2 FROM union_table_test_n3
  UNION ALL
  SELECT k,'' as d1,'' as d2 FROM union_table_test_n4) u0
LEFT OUTER JOIN union_table_test_n5 tx1 ON (u0.k = tx1.k AND tx1.k != d1) AND u0.k!=1;

DROP TABLE union_table_test_n3;
DROP TABLE union_table_test_n4;
DROP TABLE union_table_test_n5;
