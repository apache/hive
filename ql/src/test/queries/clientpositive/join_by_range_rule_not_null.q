--! qt:dataset:src1
--! qt:dataset:src

EXPLAIN SELECT * FROM src a JOIN src1 b ON a.key = b.key;
EXPLAIN SELECT * FROM src a JOIN src1 b ON a.key < b.key;
EXPLAIN SELECT * FROM src a JOIN src1 b ON a.key = b.key AND a.value >= b.value;
EXPLAIN SELECT * FROM src a JOIN src1 b ON a.key > b.value;
EXPLAIN SELECT * FROM src a JOIN src1 b ON a.key > b.key OR 1 = 1;
EXPLAIN SELECT * FROM src a JOIN src1 b ON a.key IS DISTINCT FROM b.key;
