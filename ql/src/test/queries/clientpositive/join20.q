EXPLAIN
SELECT * FROM src src1 JOIN src src2 ON (src1.key = src2.key AND src1.key < 10) RIGHT OUTER JOIN src src3 ON (src1.key = src3.key AND src3.key < 20);

SELECT * FROM src src1 JOIN src src2 ON (src1.key = src2.key AND src1.key < 10) RIGHT OUTER JOIN src src3 ON (src1.key = src3.key AND src3.key < 20);


EXPLAIN
SELECT * FROM src src1 JOIN src src2 ON (src1.key = src2.key AND src1.key < 10 AND src2.key < 15) RIGHT OUTER JOIN src src3 ON (src1.key = src3.key AND src3.key < 20);

SELECT * FROM src src1 JOIN src src2 ON (src1.key = src2.key AND src1.key < 10 AND src2.key < 15) RIGHT OUTER JOIN src src3 ON (src1.key = src3.key AND src3.key < 20);
