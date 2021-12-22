set hive.cbo.enable = True;
set hive.vectorized.execution.enabled = True;

CREATE TABLE add_part_test (key STRING, value STRING) PARTITIONED BY (ds STRING);
explain ddl select * from add_part_test;

ALTER TABLE add_part_test ADD PARTITION (ds='2010-01-01');

insert into add_part_test partition(ds='2010-01-01') values ('key_1','value_1'),
('key_2','value_2'),
('key_3','value_3'),
('key_4','value_4'),
('key_5','value_5'),
('key_6','value_6'),
('key_7','value_7'),
('key_8','value_8'),
('key_9','value_9'),
('key_10','value_10'),
('key_11','value_11'),
('key_12','value_12'),
('key_13','value_13'),
('key_14','value_14'),
('key_15','value_15'),
('key_16','value_16'),
('key_17','value_17'),
('key_18','value_18'),
('key_19','value_19');


explain ddl select * from add_part_test where ds='2010-01-01';

ALTER TABLE add_part_test ADD IF NOT EXISTS PARTITION (ds='2010-01-01') PARTITION (ds='2010-01-02') PARTITION (ds='2010-01-03');

insert into add_part_test partition(ds='2010-01-03')  values ('key_20','value_20'),
('key_21','value_21'),
('key_22','value_22'),
('key_23','value_23'),
('key_24','value_24'),
('key_25','value_25'),
('key_26','value_26'),
('key_32','value_32'),
('key_33','value_33'),
('key_34','value_34'),
('key_35','value_35'),
('key_36','value_36'),
('key_37','value_37'),
('key_38','value_38'),
('key_39','value_39');

explain ddl select * from add_part_test where ds>='2010-01-01';

DROP TABLE add_part_test;

-- Test ALTER TABLE ADD PARTITION in non-default Database

CREATE DATABASE add_part_test_db;

CREATE TABLE add_part_test_db.add_part_test (key STRING, value STRING) PARTITIONED BY (ds STRING);

ALTER TABLE add_part_test_db.add_part_test ADD PARTITION (ds='2010-01-01');

insert into add_part_test_db.add_part_test partition(ds='2010-01-01') values ('key_1','value_1'),
('key_2','value_2'),
('key_3','value_3'),
('key_4','value_4'),
('key_5','value_5'),
('key_6','value_6'),
('key_7','value_7'),
('key_8','value_8'),
('key_9','value_9'),
('key_10','value_10'),
('key_11','value_11'),
('key_12','value_12'),
('key_13','value_13'),
('key_14','value_14'),
('key_15','value_15'),
('key_16','value_16'),
('key_17','value_17'),
('key_18','value_18'),
('key_19','value_19');


explain ddl select * from add_part_test_db.add_part_test where ds='2010-01-01';

ALTER TABLE add_part_test_db.add_part_test ADD IF NOT EXISTS PARTITION (ds='2010-01-01') PARTITION (ds='2010-01-02') PARTITION (ds='2010-01-03');

insert into add_part_test_db.add_part_test partition(ds='2010-01-03')  values ('key_20','value_20'),
('key_21','value_21'),
('key_22','value_22'),
('key_23','value_23'),
('key_24','value_24'),
('key_25','value_25'),
('key_26','value_26'),
('key_32','value_32'),
('key_33','value_33'),
('key_34','value_34'),
('key_35','value_35'),
('key_36','value_36'),
('key_37','value_37'),
('key_38','value_38'),
('key_39','value_39');

explain ddl select * from add_part_test_db.add_part_test where ds>='2010-01-01';

analyze table  add_part_test_db.add_part_test compute statistics for columns;

explain ddl select * from add_part_test_db.add_part_test;

DROP DATABASE add_part_test_db cascade;

create database db_bdpbase;

CREATE TABLE db_bdpbase.emp_sports(
  id INT,
  firstname STRING,
  lastname STRING,
  sports STRING,
  city STRING,
  country STRING
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '../../data/files/part_data_explain_ddl.csv' INTO table db_bdpbase.emp_sports;

CREATE TABLE DB_BDPBASE.DEFAULT_PARTITION_TEST(
  ID    INT,
  FIRSTNAME   STRING,
  LASTNAME STRING,
  CITY STRING,
  COUNTRY  STRING
) PARTITIONED BY (
  SPORTS STRING
) ROW FORMAT DELIMITED
FIELDS TERMINATED  BY  ',';

INSERT OVERWRITE TABLE DB_BDPBASE.DEFAULT_PARTITION_TEST PARTITION (SPORTS)
  SELECT ID, FIRSTNAME, LASTNAME, CITY, COUNTRY, SPORTS FROM DB_BDPBASE.EMP_SPORTS;

analyze table db_bdpbase.default_partition_test compute statistics for columns;

explain ddl select * from db_bdpbase.default_partition_test;

drop database db_bdpbase cascade;