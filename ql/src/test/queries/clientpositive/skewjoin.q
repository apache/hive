--! qt:dataset:src1
--! qt:dataset:src
SET hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.optimize.skewjoin = true;
set hive.skewjoin.key = 2;

-- SORT_QUERY_RESULTS

CREATE TABLE T1_n128(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T2_n76(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T3_n30(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T4_n17(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE dest_j1_n17(key INT, value STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n128;
LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n76;
LOAD DATA LOCAL INPATH '../../data/files/T3.txt' INTO TABLE T3_n30;
LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T4_n17;

EXPLAIN
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1_n17 SELECT src1.key, src2.value;

FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1_n17 SELECT src1.key, src2.value;

SELECT sum(hash(key)), sum(hash(value)) FROM dest_j1_n17;
set hive.cbo.enable=false;
EXPLAIN
SELECT /*+ STREAMTABLE(a) */ *
FROM T1_n128 a JOIN T2_n76 b ON a.key = b.key
          JOIN T3_n30 c ON b.key = c.key
          JOIN T4_n17 d ON c.key = d.key;

SELECT /*+ STREAMTABLE(a) */ *
FROM T1_n128 a JOIN T2_n76 b ON a.key = b.key
          JOIN T3_n30 c ON b.key = c.key
          JOIN T4_n17 d ON c.key = d.key;

EXPLAIN
SELECT /*+ STREAMTABLE(a,c) */ *
FROM T1_n128 a JOIN T2_n76 b ON a.key = b.key
          JOIN T3_n30 c ON b.key = c.key
          JOIN T4_n17 d ON c.key = d.key;

SELECT /*+ STREAMTABLE(a,c) */ *
FROM T1_n128 a JOIN T2_n76 b ON a.key = b.key
          JOIN T3_n30 c ON b.key = c.key
          JOIN T4_n17 d ON c.key = d.key;

EXPLAIN FROM T1_n128 a JOIN src c ON c.key+1=a.key SELECT /*+ STREAMTABLE(a) */ sum(hash(a.key)), sum(hash(a.val)), sum(hash(c.key));
FROM T1_n128 a JOIN src c ON c.key+1=a.key SELECT /*+ STREAMTABLE(a) */ sum(hash(a.key)), sum(hash(a.val)), sum(hash(c.key));

EXPLAIN FROM 
(SELECT src.* FROM src) x
JOIN 
(SELECT src.* FROM src) Y
ON (x.key = Y.key)
SELECT sum(hash(Y.key)), sum(hash(Y.value));

FROM 
(SELECT src.* FROM src) x
JOIN 
(SELECT src.* FROM src) Y
ON (x.key = Y.key)
SELECT sum(hash(Y.key)), sum(hash(Y.value));

EXPLAIN FROM 
(SELECT src.* FROM src) x
JOIN 
(SELECT src.* FROM src) Y
ON (x.key = Y.key and substring(x.value, 5)=substring(y.value, 5)+1)
SELECT sum(hash(Y.key)), sum(hash(Y.value));

FROM 
(SELECT src.* FROM src) x
JOIN 
(SELECT src.* FROM src) Y
ON (x.key = Y.key and substring(x.value, 5)=substring(y.value, 5)+1)
SELECT sum(hash(Y.key)), sum(hash(Y.value));

EXPLAIN
SELECT sum(hash(src1.c1)), sum(hash(src2.c4)) 
FROM
(SELECT src.key as c1, src.value as c2 from src) src1
JOIN
(SELECT src.key as c3, src.value as c4 from src) src2
ON src1.c1 = src2.c3 AND src1.c1 < 100
JOIN
(SELECT src.key as c5, src.value as c6 from src) src3
ON src1.c1 = src3.c5 AND src3.c5 < 80;

SELECT sum(hash(src1.c1)), sum(hash(src2.c4))
FROM
(SELECT src.key as c1, src.value as c2 from src) src1
JOIN
(SELECT src.key as c3, src.value as c4 from src) src2
ON src1.c1 = src2.c3 AND src1.c1 < 100
JOIN
(SELECT src.key as c5, src.value as c6 from src) src3
ON src1.c1 = src3.c5 AND src3.c5 < 80;

EXPLAIN
SELECT /*+ mapjoin(v)*/ sum(hash(k.key)), sum(hash(v.val)) FROM T1_n128 k LEFT OUTER JOIN T1_n128 v ON k.key+1=v.key;
SELECT /*+ mapjoin(v)*/ sum(hash(k.key)), sum(hash(v.val)) FROM T1_n128 k LEFT OUTER JOIN T1_n128 v ON k.key+1=v.key;

select /*+ mapjoin(k)*/ sum(hash(k.key)), sum(hash(v.val)) from T1_n128 k join T1_n128 v on k.key=v.val;

select /*+ mapjoin(k)*/ sum(hash(k.key)), sum(hash(v.val)) from T1_n128 k join T1_n128 v on k.key=v.key;

select sum(hash(k.key)), sum(hash(v.val)) from T1_n128 k join T1_n128 v on k.key=v.key;

select count(1) from  T1_n128 a join T1_n128 b on a.key = b.key;

FROM T1_n128 a LEFT OUTER JOIN T2_n76 c ON c.key+1=a.key SELECT sum(hash(a.key)), sum(hash(a.val)), sum(hash(c.key));

FROM T1_n128 a RIGHT OUTER JOIN T2_n76 c ON c.key+1=a.key SELECT /*+ STREAMTABLE(a) */ sum(hash(a.key)), sum(hash(a.val)), sum(hash(c.key));

FROM T1_n128 a FULL OUTER JOIN T2_n76 c ON c.key+1=a.key SELECT /*+ STREAMTABLE(a) */ sum(hash(a.key)), sum(hash(a.val)), sum(hash(c.key));

SELECT sum(hash(src1.key)), sum(hash(src1.val)), sum(hash(src2.key)) FROM T1_n128 src1 LEFT OUTER JOIN T2_n76 src2 ON src1.key+1 = src2.key RIGHT OUTER JOIN T2_n76 src3 ON src2.key = src3.key;

SELECT sum(hash(src1.key)), sum(hash(src1.val)), sum(hash(src2.key)) FROM T1_n128 src1 JOIN T2_n76 src2 ON src1.key+1 = src2.key JOIN T2_n76 src3 ON src2.key = src3.key;

select /*+ mapjoin(v)*/ sum(hash(k.key)), sum(hash(v.val)) from T1_n128 k left outer join T1_n128 v on k.key+1=v.key;






