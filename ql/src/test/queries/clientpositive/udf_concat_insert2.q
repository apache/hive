--! qt:dataset:src
CREATE TABLE dest1_n108(key STRING, value STRING) STORED AS TEXTFILE;

FROM src
INSERT OVERWRITE TABLE dest1_n108 SELECT concat('1234', 'abc', 'extra argument'), src.value WHERE src.key < 100;

SELECT dest1_n108.* FROM dest1_n108;


