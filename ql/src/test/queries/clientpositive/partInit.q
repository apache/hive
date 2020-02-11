set hive.mapred.mode=nonstrict;
CREATE TABLE empty_n1 (c INT) PARTITIONED BY (p INT);
SELECT MAX(c) FROM empty_n1;
SELECT MAX(p) FROM empty_n1;

ALTER TABLE empty_n1 ADD PARTITION (p=1);

set hive.optimize.metadataonly=true;
SELECT MAX(p) FROM empty_n1;

set hive.optimize.metadataonly=false;
SELECT MAX(p) FROM empty_n1;

