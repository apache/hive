--! qt:dataset:src1
CREATE TABLE dest1_n35(key STRING, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src1
INSERT OVERWRITE TABLE dest1_n35 SELECT src1.key, src1.value WHERE src1.key is null;

FROM src1
INSERT OVERWRITE TABLE dest1_n35 SELECT src1.key, src1.value WHERE src1.key is null;

SELECT dest1_n35.* FROM dest1_n35;
