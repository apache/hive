--! qt:dataset:src1
--! qt:dataset:src
EXPLAIN
SELECT key from src JOIN src1 on src1.key=src.key;

SELECT key from src JOIN src1 on src1.key=src.key;
