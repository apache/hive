--! qt:dataset:src
-- SORT_QUERY_RESULTS

CREATE TABLE dest1_n139(key INT, value STRING) STORED AS TEXTFILE;

FROM src
INSERT OVERWRITE TABLE dest1_n139 SELECT '1234', concat(src.key) WHERE src.key < 100 group by src.key;

SELECT dest1_n139.* FROM dest1_n139;

