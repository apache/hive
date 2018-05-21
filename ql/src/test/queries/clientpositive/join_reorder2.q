
set hive.cbo.enable=false;




CREATE TABLE T1_n49(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T2_n30(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T3_n10(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T4_n3(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n49;
LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n30;
LOAD DATA LOCAL INPATH '../../data/files/T3.txt' INTO TABLE T3_n10;
LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T4_n3;

EXPLAIN
SELECT /*+ STREAMTABLE(a) */ *
FROM T1_n49 a JOIN T2_n30 b ON a.key = b.key
          JOIN T3_n10 c ON b.key = c.key
          JOIN T4_n3 d ON c.key = d.key;

SELECT /*+ STREAMTABLE(a) */ *
FROM T1_n49 a JOIN T2_n30 b ON a.key = b.key
          JOIN T3_n10 c ON b.key = c.key
          JOIN T4_n3 d ON c.key = d.key;


EXPLAIN
SELECT /*+ STREAMTABLE(a) */ *
FROM T1_n49 a JOIN T2_n30 b ON a.key = b.key
          JOIN T3_n10 c ON a.val = c.val
          JOIN T4_n3 d ON a.key + 1 = d.key + 1;


SELECT /*+ STREAMTABLE(a) */ *
FROM T1_n49 a JOIN T2_n30 b ON a.key = b.key
          JOIN T3_n10 c ON a.val = c.val
          JOIN T4_n3 d ON a.key + 1 = d.key + 1;






