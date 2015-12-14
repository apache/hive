set hive.mapred.mode=nonstrict;
EXPLAIN
SELECT x.* FROM SRC x WHERE x.key < 300 LIMIT 5;

SELECT x.* FROM SRC x WHERE x.key < 300 LIMIT 5;
