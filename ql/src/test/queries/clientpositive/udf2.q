--! qt:dataset:src
CREATE TABLE dest1_n55(c1 STRING) STORED AS TEXTFILE;

FROM src INSERT OVERWRITE TABLE dest1_n55 SELECT '  abc  ' WHERE src.key = 86;

EXPLAIN
SELECT '|', trim(dest1_n55.c1), '|', rtrim(dest1_n55.c1), '|', ltrim(dest1_n55.c1), '|' FROM dest1_n55;

SELECT '|', trim(dest1_n55.c1), '|', rtrim(dest1_n55.c1), '|', ltrim(dest1_n55.c1), '|' FROM dest1_n55;
