set hive.mapred.mode=nonstrict;
CREATE TABLE empty (c INT) PARTITIONED BY (p INT);
SELECT MAX(c) FROM empty;
SELECT MAX(p) FROM empty;

ALTER TABLE empty ADD PARTITION (p=1);

set hive.optimize.metadataonly=true;
SELECT MAX(p) FROM empty;

set hive.optimize.metadataonly=false;
SELECT MAX(p) FROM empty;

