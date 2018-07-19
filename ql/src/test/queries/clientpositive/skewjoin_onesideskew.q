set hive.auto.convert.join=false;
set hive.optimize.skewjoin=true;
set hive.skewjoin.key=2;
set hive.mapred.mode=nonstrict;

DROP TABLE IF EXISTS skewtable;
CREATE TABLE skewtable (key STRING, value STRING) STORED AS TEXTFILE;
INSERT INTO TABLE skewtable VALUES ("0", "val_0");
INSERT INTO TABLE skewtable VALUES ("0", "val_0");
INSERT INTO TABLE skewtable VALUES ("0", "val_0");

DROP TABLE IF EXISTS nonskewtable;
CREATE TABLE nonskewtable (key STRING, value STRING) STORED AS TEXTFILE;
INSERT INTO TABLE nonskewtable VALUES ("1", "val_1");
INSERT INTO TABLE nonskewtable VALUES ("2", "val_2");

EXPLAIN
CREATE TABLE result_n1 AS SELECT a.* FROM skewtable a JOIN nonskewtable b ON a.key=b.key;
CREATE TABLE result_n1 AS SELECT a.* FROM skewtable a JOIN nonskewtable b ON a.key=b.key;

SELECT * FROM result_n1;

