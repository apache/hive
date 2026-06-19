--! qt:dataset:src1
--! qt:dataset:src
FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key + src2.key = src3.key)
INSERT OVERWRITE TABLE dest1 SELECT src1.key, src3.value

