--! qt:dataset:src1
FROM src1 
SELECT 4 + NULL, src1.key - NULL, NULL + NULL
