--! qt:dataset:src
set hive.mapred.mode=nonstrict;
CREATE TABLE dest1_n5(c1 STRING) STORED AS TEXTFILE;

EXPLAIN
INSERT OVERWRITE TABLE dest1_n5
SELECT trim(trim(trim(trim(trim(trim(trim(trim(trim(trim( '  abc  '))))))))))
FROM src
WHERE src.key = 86;

INSERT OVERWRITE TABLE dest1_n5
SELECT trim(trim(trim(trim(trim(trim(trim(trim(trim(trim( '  abc  '))))))))))
FROM src
WHERE src.key = 86;
