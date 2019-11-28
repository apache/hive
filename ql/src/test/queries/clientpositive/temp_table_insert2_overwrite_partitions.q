set hive.mapred.mode=nonstrict;
CREATE DATABASE db1;

CREATE DATABASE db2;

CREATE TEMPORARY TABLE db1.sourceTable_temp (one string,two string) PARTITIONED BY (ds string);

load data local inpath '../../data/files/kv1.txt' INTO TABLE db1.sourceTable_temp partition(ds='2011-11-11');

load data local inpath '../../data/files/kv3.txt' INTO TABLE db1.sourceTable_temp partition(ds='2011-11-11');

CREATE TEMPORARY TABLE db2.destinTable_temp (one string,two string) PARTITIONED BY (ds string);

EXPLAIN INSERT OVERWRITE TABLE db2.destinTable_temp PARTITION (ds='2011-11-11')
  SELECT one,two FROM db1.sourceTable_temp WHERE ds='2011-11-11' order by one desc, two desc limit 5;

INSERT OVERWRITE TABLE db2.destinTable_temp PARTITION (ds='2011-11-11')
  SELECT one,two FROM db1.sourceTable_temp WHERE ds='2011-11-11' order by one desc, two desc limit 5;

select one,two from db2.destinTable_temp order by one desc, two desc;

EXPLAIN INSERT OVERWRITE TABLE db2.destinTable_temp PARTITION (ds='2011-11-11')
  SELECT one,two FROM db1.sourceTable_temp WHERE ds='2011-11-11' order by one desc, two desc limit 5;

INSERT OVERWRITE TABLE db2.destinTable_temp PARTITION (ds='2011-11-11')
  SELECT one,two FROM db1.sourceTable_temp WHERE ds='2011-11-11' order by one desc, two desc limit 5;

select one,two from db2.destinTable_temp order by one desc, two desc;

drop table db2.destinTable_temp;

drop table db1.sourceTable_temp;

DROP DATABASE db1;

DROP DATABASE db2;
