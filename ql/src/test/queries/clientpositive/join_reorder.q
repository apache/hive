--! qt:dataset:src
set hive.mapred.mode=nonstrict;

set hive.cbo.enable=false;



CREATE TABLE T1_n37(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T2_n24(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T3_n8(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n37;
LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n24;
LOAD DATA LOCAL INPATH '../../data/files/T3.txt' INTO TABLE T3_n8;

-- SORT_QUERY_RESULTS

EXPLAIN FROM T1_n37 a JOIN src c ON c.key+1=a.key
SELECT a.key, a.val, c.key;

EXPLAIN FROM T1_n37 a JOIN src c ON c.key+1=a.key
SELECT /*+ STREAMTABLE(a) */ a.key, a.val, c.key;

FROM T1_n37 a JOIN src c ON c.key+1=a.key
SELECT a.key, a.val, c.key;

FROM T1_n37 a JOIN src c ON c.key+1=a.key
SELECT /*+ STREAMTABLE(a) */ a.key, a.val, c.key;

EXPLAIN FROM T1_n37 a
  LEFT OUTER JOIN T2_n24 b ON (b.key=a.key)
  RIGHT OUTER JOIN T3_n8 c ON (c.val = a.val)
SELECT a.key, b.key, a.val, c.val;

EXPLAIN FROM T1_n37 a
  LEFT OUTER JOIN T2_n24 b ON (b.key=a.key)
  RIGHT OUTER JOIN T3_n8 c ON (c.val = a.val)
SELECT /*+ STREAMTABLE(a) */ a.key, b.key, a.val, c.val;

FROM T1_n37 a
  LEFT OUTER JOIN T2_n24 b ON (b.key=a.key)
  RIGHT OUTER JOIN T3_n8 c ON (c.val = a.val)
SELECT a.key, b.key, a.val, c.val;

FROM T1_n37 a
  LEFT OUTER JOIN T2_n24 b ON (b.key=a.key)
  RIGHT OUTER JOIN T3_n8 c ON (c.val = a.val)
SELECT /*+ STREAMTABLE(a) */ a.key, b.key, a.val, c.val;

EXPLAIN FROM UNIQUEJOIN
  PRESERVE T1_n37 a (a.key, a.val),
  PRESERVE T2_n24 b (b.key, b.val),
  PRESERVE T3_n8 c (c.key, c.val)
SELECT a.key, b.key, c.key;

EXPLAIN FROM UNIQUEJOIN
  PRESERVE T1_n37 a (a.key, a.val),
  PRESERVE T2_n24 b (b.key, b.val),
  PRESERVE T3_n8 c (c.key, c.val)
SELECT /*+ STREAMTABLE(b) */ a.key, b.key, c.key;

FROM UNIQUEJOIN
  PRESERVE T1_n37 a (a.key, a.val),
  PRESERVE T2_n24 b (b.key, b.val),
  PRESERVE T3_n8 c (c.key, c.val)
SELECT a.key, b.key, c.key;

FROM UNIQUEJOIN
  PRESERVE T1_n37 a (a.key, a.val),
  PRESERVE T2_n24 b (b.key, b.val),
  PRESERVE T3_n8 c (c.key, c.val)
SELECT /*+ STREAMTABLE(b) */ a.key, b.key, c.key;




