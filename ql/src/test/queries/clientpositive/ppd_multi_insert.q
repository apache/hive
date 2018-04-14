--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

-- SORT_QUERY_RESULTS

CREATE TABLE mi1(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE mi2(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE mi3(key INT) PARTITIONED BY(ds STRING, hr STRING) STORED AS TEXTFILE;
CREATE TABLE mi4(value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src a JOIN src b ON (a.key = b.key)
INSERT OVERWRITE TABLE mi1 SELECT a.* WHERE a.key < 100
INSERT OVERWRITE TABLE mi2 SELECT a.key, a.value WHERE a.key >= 100 and a.key < 200
INSERT OVERWRITE TABLE mi3 PARTITION(ds='2008-04-08', hr='12') SELECT a.key WHERE a.key >= 200 and a.key < 300
INSERT OVERWRITE DIRECTORY 'target/warehouse/mi4.out' SELECT a.value WHERE a.key >= 300;

FROM src a JOIN src b ON (a.key = b.key)
INSERT OVERWRITE TABLE mi1 SELECT a.* WHERE a.key < 100
INSERT OVERWRITE TABLE mi2 SELECT a.key, a.value WHERE a.key >= 100 and a.key < 200
INSERT OVERWRITE TABLE mi3 PARTITION(ds='2008-04-08', hr='12') SELECT a.key WHERE a.key >= 200 and a.key < 300
INSERT OVERWRITE DIRECTORY 'target/warehouse/mi4.out' SELECT a.value WHERE a.key >= 300;

SELECT mi1.* FROM mi1;
SELECT mi2.* FROM mi2;
SELECT mi3.* FROM mi3;
LOAD DATA INPATH '${system:test.warehouse.dir}/mi4.out' OVERWRITE INTO TABLE mi4;
SELECT mi4.* FROM mi4;


set hive.ppd.remove.duplicatefilters=true;

EXPLAIN
FROM src a JOIN src b ON (a.key = b.key)
INSERT OVERWRITE TABLE mi1 SELECT a.* WHERE a.key < 100
INSERT OVERWRITE TABLE mi2 SELECT a.key, a.value WHERE a.key >= 100 and a.key < 200
INSERT OVERWRITE TABLE mi3 PARTITION(ds='2008-04-08', hr='12') SELECT a.key WHERE a.key >= 200 and a.key < 300
INSERT OVERWRITE DIRECTORY 'target/warehouse/mi4.out' SELECT a.value WHERE a.key >= 300;

FROM src a JOIN src b ON (a.key = b.key)
INSERT OVERWRITE TABLE mi1 SELECT a.* WHERE a.key < 100
INSERT OVERWRITE TABLE mi2 SELECT a.key, a.value WHERE a.key >= 100 and a.key < 200
INSERT OVERWRITE TABLE mi3 PARTITION(ds='2008-04-08', hr='12') SELECT a.key WHERE a.key >= 200 and a.key < 300
INSERT OVERWRITE DIRECTORY 'target/warehouse/mi4.out' SELECT a.value WHERE a.key >= 300;

SELECT mi1.* FROM mi1;
SELECT mi2.* FROM mi2;
SELECT mi3.* FROM mi3;
LOAD DATA INPATH '${system:test.warehouse.dir}/mi4.out' OVERWRITE INTO TABLE mi4;
SELECT mi4.* FROM mi4;
