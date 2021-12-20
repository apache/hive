CREATE TABLE join_1to1_1(key1 int, key2 int, value int);
COPY join_1to1_2 FROM '/tmp/in6.txt' ( FORMAT CSV, DELIMITER(E'\1') );

