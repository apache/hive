CREATE TABLE join_1to1_1(key1 int, key2 int, value int);
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/in5.txt' INTO TABLE join_1to1_1;

