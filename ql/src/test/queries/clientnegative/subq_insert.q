--! qt:dataset:src1
--! qt:dataset:src
EXPLAIN
SELECT * FROM (INSERT OVERWRITE TABLE src1 SELECT * FROM src ) y;
