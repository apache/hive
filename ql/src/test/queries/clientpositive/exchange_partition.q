create database ex1;
create database ex2;

CREATE TABLE ex1.exchange_part_test1 (f1 string) PARTITIONED BY (ds STRING);
CREATE TABLE ex2.exchange_part_test2 (f1 string) PARTITIONED BY (ds STRING);
SHOW PARTITIONS ex1.exchange_part_test1;
SHOW PARTITIONS ex2.exchange_part_test2;

ALTER TABLE ex2.exchange_part_test2 ADD PARTITION (ds='2013-04-05');
SHOW PARTITIONS ex1.exchange_part_test1;
SHOW PARTITIONS ex2.exchange_part_test2;

ALTER TABLE ex1.exchange_part_test1 EXCHANGE PARTITION (ds='2013-04-05') WITH TABLE ex2.exchange_part_test2;
SHOW PARTITIONS ex1.exchange_part_test1;
SHOW PARTITIONS ex2.exchange_part_test2;
