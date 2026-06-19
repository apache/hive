-- Test overflow handling in computeOnlineDataSize with Long.MAX_VALUE statistics

SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=10000000;

CREATE TABLE t1 (k BIGINT, v STRING);
CREATE TABLE t2 (k BIGINT, v STRING);

-- Case 1: Normal statistics - t1 fits in 10MB threshold, MapJoin expected
ALTER TABLE t1 UPDATE STATISTICS SET('numRows'='10000','rawDataSize'='100000');
ALTER TABLE t1 UPDATE STATISTICS FOR COLUMN k SET('numDVs'='10000','numNulls'='0');
ALTER TABLE t1 UPDATE STATISTICS FOR COLUMN v SET('numDVs'='10000','numNulls'='0','avgColLen'='10.0','maxColLen'='20');

ALTER TABLE t2 UPDATE STATISTICS SET('numRows'='1000000','rawDataSize'='10000000');
ALTER TABLE t2 UPDATE STATISTICS FOR COLUMN k SET('numDVs'='1000000','numNulls'='0');
ALTER TABLE t2 UPDATE STATISTICS FOR COLUMN v SET('numDVs'='1000000','numNulls'='0','avgColLen'='10.0','maxColLen'='20');

EXPLAIN SELECT t1.k, t2.v FROM t1 JOIN t2 ON t1.k = t2.k;

-- Case 2: Long.MAX_VALUE numRows - without fix, overflow causes negative size and incorrect MapJoin
ALTER TABLE t1 UPDATE STATISTICS SET('numRows'='9223372036854775807','rawDataSize'='9223372036854775807');
ALTER TABLE t1 UPDATE STATISTICS FOR COLUMN k SET('numDVs'='1000','numNulls'='0');
ALTER TABLE t1 UPDATE STATISTICS FOR COLUMN v SET('numDVs'='1000','numNulls'='0','avgColLen'='10.0','maxColLen'='20');

EXPLAIN SELECT t1.k, t1.v, t2.v FROM t1 JOIN t2 ON t1.k = t2.k;
