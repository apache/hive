
CREATE TABLE alter_table_location3 (id int, name string, dept string)
  PARTITIONED BY (year int)
  STORED AS TEXTFILE
  LOCATION 'pfile://${system:test.tmp.dir}/alter_table_location3'
;

INSERT INTO alter_table_location3 PARTITION (year=2016) VALUES (8,'Henry','CSE');
ALTER TABLE alter_table_location3 ADD PARTITION (year=2017);

ALTER TABLE alter_table_location3 SET LOCATION 'hdfs:///tmp/alter_table_location3';
INSERT INTO alter_table_location3 PARTITION (year=2016) VALUES (9,'Horace','CSE');
INSERT INTO alter_table_location3 PARTITION (year=2017) VALUES (10,'Harris','CSE');
INSERT INTO alter_table_location3 PARTITION (year=2018) VALUES (11,'Humphrey','CSE');

SELECT * from alter_table_location3 order by id;
