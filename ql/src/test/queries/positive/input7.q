--! qt:dataset:src1
FROM src1
INSERT OVERWRITE TABLE dest1 SELECT NULL, src1.key
