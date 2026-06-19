set hive.cbo.enable=false;




CREATE TABLE T1_n92(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T2_n57(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T3_n21(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T4_n10(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n92;
LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n57;
LOAD DATA LOCAL INPATH '../../data/files/T3.txt' INTO TABLE T3_n21;
LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T4_n10;

EXPLAIN
SELECT /*+ STREAMTABLE(a,c) */ *
FROM T1_n92 a JOIN T2_n57 b ON a.key = b.key
          JOIN T3_n21 c ON b.key = c.key
          JOIN T4_n10 d ON c.key = d.key;

SELECT /*+ STREAMTABLE(a,c) */ *
FROM T1_n92 a JOIN T2_n57 b ON a.key = b.key
          JOIN T3_n21 c ON b.key = c.key
          JOIN T4_n10 d ON c.key = d.key;


EXPLAIN
SELECT /*+ STREAMTABLE(a,c) */ *
FROM T1_n92 a JOIN T2_n57 b ON a.key = b.key
          JOIN T3_n21 c ON a.val = c.val
          JOIN T4_n10 d ON a.key + 1 = d.key + 1;


SELECT /*+ STREAMTABLE(a,c) */ *
FROM T1_n92 a JOIN T2_n57 b ON a.key = b.key
          JOIN T3_n21 c ON a.val = c.val
          JOIN T4_n10 d ON a.key + 1 = d.key + 1;






