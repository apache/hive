-- SORT_QUERY_RESULTS

SET hive.strict.checks.bucketing=true;
SET hive.auto.convert.join=true;
SET hive.auto.convert.sortmerge.join=true;
SET hive.optimize.bucketmapjoin = true;
SET hive.optimize.bucketmapjoin.sortedmerge = true;
SET hive.auto.convert.join.noconditionaltask.size=1;
SET hive.optimize.dynamic.partition.hashjoin=false;

DROP TABLE IF EXISTS bucket2;
CREATE TABLE bucket2(key string, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

DROP TABLE IF EXISTS bucket3;
CREATE TABLE bucket3(key string, value string) CLUSTERED BY (key) SORTED BY (key) INTO 3 BUCKETS;

INSERT INTO TABLE bucket2 VALUES (1, 1), (2, 2), (7, 7), (6, 6), (14, 14), (11, 11);
INSERT INTO TABLE bucket3 VALUES (1, 1), (2, 2), (7, 7), (6, 6), (14, 14), (11, 11);

EXPLAIN
SELECT * FROM bucket2 JOIN bucket3 on bucket2.key = bucket3.key;
SELECT * FROM bucket2 JOIN bucket3 on bucket2.key = bucket3.key;

