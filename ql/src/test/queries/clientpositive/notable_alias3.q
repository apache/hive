--! qt:dataset:src
CREATE TABLE dest1_n102(c string, key INT, value DOUBLE) STORED AS TEXTFILE;

FROM src
INSERT OVERWRITE TABLE dest1_n102 SELECT '1234', src.key, sum(src.value) WHERE src.key < 100 group by key;