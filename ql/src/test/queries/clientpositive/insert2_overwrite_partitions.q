set hive.mapred.mode=nonstrict;
CREATE DATABASE db1;

CREATE DATABASE db2;

CREATE TABLE db1.sourceTable (one string,two string) PARTITIONED BY (ds string);

load data local inpath '../../data/files/kv1.txt' INTO TABLE db1.sourceTable partition(ds='2011-11-11');

load data local inpath '../../data/files/kv3.txt' INTO TABLE db1.sourceTable partition(ds='2011-11-11');

CREATE TABLE db2.destinTable (one string,two string) PARTITIONED BY (ds string);

EXPLAIN INSERT OVERWRITE TABLE db2.destinTable PARTITION (ds='2011-11-11')
SELECT one,two FROM db1.sourceTable WHERE ds='2011-11-11' order by one desc, two desc limit 5;

INSERT OVERWRITE TABLE db2.destinTable PARTITION (ds='2011-11-11')
SELECT one,two FROM db1.sourceTable WHERE ds='2011-11-11' order by one desc, two desc limit 5;

select one,two from db2.destinTable order by one desc, two desc;

EXPLAIN INSERT OVERWRITE TABLE db2.destinTable PARTITION (ds='2011-11-11')
SELECT one,two FROM db1.sourceTable WHERE ds='2011-11-11' order by one desc, two desc limit 5;

INSERT OVERWRITE TABLE db2.destinTable PARTITION (ds='2011-11-11')
SELECT one,two FROM db1.sourceTable WHERE ds='2011-11-11' order by one desc, two desc limit 5;

select one,two from db2.destinTable order by one desc, two desc;

INSERT OVERWRITE TABLE db2.destinTable PARTITION (ds='2011-11-11') IF NOT EXISTS SELECT 100, 200;
-- Ensure case sensitivity does not make a difference
INSERT OVERWRITE TABLE DB2.DESTINTABLE PARTITION (ds='2011-11-11') IF NOT EXISTS SELECT 100, 200;
INSERT OVERWRITE TABLE db2.destinTable PARTITION (ds='2012-11-11') IF NOT EXISTS SELECT 900, 800;
-- Ensure case sensitivity does not make a difference
INSERT OVERWRITE TABLE DB2.DESTINTABLE PARTITION (ds='2013-11-11') IF NOT EXISTS SELECT 950, 850;

select one,two from db2.destinTable order by one desc, two desc;

drop table db2.destinTable;

drop table db1.sourceTable;

DROP DATABASE db1;

DROP DATABASE db2;
