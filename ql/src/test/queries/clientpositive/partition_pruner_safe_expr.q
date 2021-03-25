CREATE TABLE t1pstring (col1 string) PARTITIONED BY (p1 string);
INSERT INTO t1pstring PARTITION (p1) VALUES ("2020","2020"),("2021","2021"),("2021_backup","2021_backup"),('8888','8888'),('88889','88889'),('8888_a','8888_a'),('a8888','a8888');

explain extended SELECT count(*) FROM t1pstring WHERE p1=8888;
