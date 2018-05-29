CREATE TABLE exchange_part_test1_n0 (f1 string) PARTITIONED BY (ds STRING, hr STRING);
CREATE TABLE exchange_part_test2_n0 (f1 string) PARTITIONED BY (ds STRING, hr STRING);
SHOW PARTITIONS exchange_part_test1_n0;
SHOW PARTITIONS exchange_part_test2_n0;

ALTER TABLE exchange_part_test1_n0 ADD PARTITION (ds='2014-01-03', hr='1');
ALTER TABLE exchange_part_test2_n0 ADD PARTITION (ds='2013-04-05', hr='1');
ALTER TABLE exchange_part_test2_n0 ADD PARTITION (ds='2013-04-05', hr='2');
SHOW PARTITIONS exchange_part_test1_n0;
SHOW PARTITIONS exchange_part_test2_n0;

-- This will exchange both partitions hr=1 and hr=2
ALTER TABLE exchange_part_test1_n0 EXCHANGE PARTITION (ds='2013-04-05') WITH TABLE exchange_part_test2_n0;
SHOW PARTITIONS exchange_part_test1_n0;
SHOW PARTITIONS exchange_part_test2_n0;
