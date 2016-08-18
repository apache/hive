-- SORT_QUERY_RESULTS

CREATE TABLE join_1to1_1(key1 int, key2 int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in5.txt' INTO TABLE join_1to1_1;

CREATE TABLE join_1to1_2(key1 int, key2 int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in6.txt' INTO TABLE join_1to1_2;


set hive.join.emit.interval=5;

SELECT * FROM join_1to1_1 a join join_1to1_2 b on a.key1 = b.key1;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.value = 66 and b.value = 66;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2 and a.value = 66 and b.value = 66;

set hive.join.emit.interval=2;
SELECT * FROM join_1to1_1 a join join_1to1_2 b on a.key1 = b.key1;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.value = 66 and b.value = 66;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2 and a.value = 66 and b.value = 66;

set hive.join.emit.interval=1;
SELECT * FROM join_1to1_1 a join join_1to1_2 b on a.key1 = b.key1;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.value = 66 and b.value = 66;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2 and a.value = 66 and b.value = 66;



set hive.join.emit.interval=5;

SELECT * FROM join_1to1_1 a join join_1to1_2 b on a.key1 = b.key1;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.value = 66 and b.value = 66;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2 and a.value = 66 and b.value = 66;

set hive.join.emit.interval=2;
SELECT * FROM join_1to1_1 a join join_1to1_2 b on a.key1 = b.key1;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.value = 66 and b.value = 66;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2 and a.value = 66 and b.value = 66;

set hive.join.emit.interval=1;
SELECT * FROM join_1to1_1 a join join_1to1_2 b on a.key1 = b.key1;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.value = 66 and b.value = 66;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2 and a.value = 66 and b.value = 66;

