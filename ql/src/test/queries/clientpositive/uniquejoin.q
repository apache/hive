CREATE TABLE T1_n1(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T2_n1(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T3_n0(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n1;
LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n1;
LOAD DATA LOCAL INPATH '../../data/files/T3.txt' INTO TABLE T3_n0;

-- SORT_QUERY_RESULTS

FROM UNIQUEJOIN PRESERVE T1_n1 a (a.key), PRESERVE T2_n1 b (b.key), PRESERVE T3_n0 c (c.key)
SELECT a.key, b.key, c.key;

FROM UNIQUEJOIN T1_n1 a (a.key), T2_n1 b (b.key), T3_n0 c (c.key)
SELECT a.key, b.key, c.key;

FROM UNIQUEJOIN T1_n1 a (a.key), T2_n1 b (b.key-1), T3_n0 c (c.key)
SELECT a.key, b.key, c.key;

FROM UNIQUEJOIN PRESERVE T1_n1 a (a.key, a.val), PRESERVE T2_n1 b (b.key, b.val), PRESERVE T3_n0 c (c.key, c.val)
SELECT a.key, a.val, b.key, b.val, c.key, c.val;

FROM UNIQUEJOIN PRESERVE T1_n1 a (a.key), T2_n1 b (b.key), PRESERVE T3_n0 c (c.key)
SELECT a.key, b.key, c.key;

FROM UNIQUEJOIN PRESERVE T1_n1 a (a.key), T2_n1 b(b.key)
SELECT a.key, b.key;
