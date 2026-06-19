--! qt:dataset:src
-- SORT_QUERY_RESULTS

CREATE TABLE dest1_n12(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE dest2_n2(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1_n12 SELECT src.key, src.value WHERE src.key < 100 LIMIT 10
INSERT OVERWRITE TABLE dest2_n2 SELECT src.key, src.value WHERE src.key < 100 LIMIT 5;

FROM src
INSERT OVERWRITE TABLE dest1_n12 SELECT src.key, src.value WHERE src.key < 100 LIMIT 10
INSERT OVERWRITE TABLE dest2_n2 SELECT src.key, src.value WHERE src.key < 100 LIMIT 5;

SELECT dest1_n12.* FROM dest1_n12;
SELECT dest2_n2.* FROM dest2_n2;




