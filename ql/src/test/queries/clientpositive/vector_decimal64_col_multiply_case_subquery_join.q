CREATE TABLE test1(col2 varchar(28), col3 decimal(26,6), col4 varchar(28));
CREATE TABLE test2(col4 varchar(28));

INSERT INTO test1 VALUES ('abc', 1.00, 'DEF');
INSERT INTO test2 VALUES ('DEF');

EXPLAIN VECTORIZATION DETAIL
SELECT
  aa.col3 * CASE WHEN aa.col2 = 'bc' THEN 1.77 ELSE 0.72 END AS int_cost
FROM test1 aa
  INNER JOIN
    test2 bb ON aa.col4 = bb.col4;

SELECT
  aa.col3 * CASE WHEN aa.col2 = 'bc' THEN 1.77 ELSE 0.72 END AS int_cost
FROM test1 aa
  INNER JOIN
    test2 bb ON aa.col4 = bb.col4;

DROP TABLE test1;
DROP TABLE test2;