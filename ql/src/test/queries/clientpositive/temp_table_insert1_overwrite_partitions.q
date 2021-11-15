set hive.mapred.mode=nonstrict;
CREATE TEMPORARY TABLE sourceTable_temp (one string,two string) PARTITIONED BY (ds string,hr string);

load data local inpath '../../data/files/kv1.txt' INTO TABLE sourceTable_temp partition(ds='2011-11-11', hr='11');

load data local inpath '../../data/files/kv3.txt' INTO TABLE sourceTable_temp partition(ds='2011-11-11', hr='12');

CREATE TEMPORARY TABLE destinTable_temp (one string,two string) PARTITIONED BY (ds string,hr string);

EXPLAIN INSERT OVERWRITE TABLE destinTable_temp PARTITION (ds='2011-11-11', hr='11') if not exists
  SELECT one,two FROM sourceTable_temp WHERE ds='2011-11-11' AND hr='11' order by one desc, two desc limit 5;

INSERT OVERWRITE TABLE destinTable_temp PARTITION (ds='2011-11-11', hr='11') if not exists
  SELECT one,two FROM sourceTable_temp WHERE ds='2011-11-11' AND hr='11' order by one desc, two desc limit 5;

select one,two from destinTable_temp order by one desc, two desc;

EXPLAIN INSERT OVERWRITE TABLE destinTable_temp PARTITION (ds='2011-11-11', hr='11') if not exists
  SELECT one,two FROM sourceTable_temp WHERE ds='2011-11-11' AND hr='12' order by one desc, two desc limit 5;

INSERT OVERWRITE TABLE destinTable_temp PARTITION (ds='2011-11-11', hr='11') if not exists
  SELECT one,two FROM sourceTable_temp WHERE ds='2011-11-11' AND hr='12' order by one desc, two desc limit 5;

select one,two from destinTable_temp order by one desc, two desc;

drop table destinTable_temp;

CREATE TEMPORARY TABLE destinTable_temp (one string,two string);

EXPLAIN INSERT OVERWRITE TABLE destinTable_temp SELECT one,two FROM sourceTable_temp WHERE ds='2011-11-11' AND hr='11' order by one desc, two desc limit 5;

INSERT OVERWRITE TABLE destinTable_temp SELECT one,two FROM sourceTable_temp WHERE ds='2011-11-11' AND hr='11' order by one desc, two desc limit 5;

drop table destinTable_temp;

drop table sourceTable_temp;

CREATE TEMPORARY TABLE sourceTable_temp (one string,two string) PARTITIONED BY (ds string,hr string);

load data local inpath '../../data/files/kv1.txt' INTO TABLE sourceTable_temp partition(ds='2011-11-11', hr='11');

CREATE TEMPORARY TABLE destinTable_temp (one string,two string) PARTITIONED BY (ds string,hr string);

EXPLAIN INSERT OVERWRITE TABLE destinTable_temp PARTITION (ds='2011-11-11', hr='11') if not exists
  SELECT one,two FROM sourceTable_temp WHERE ds='2011-11-11' AND hr='11' order by one desc, two desc limit 5;

INSERT OVERWRITE TABLE destinTable_temp PARTITION (ds='2011-11-11', hr='11') if not exists
  SELECT one,two FROM sourceTable_temp WHERE ds='2011-11-11' AND hr='11' order by one desc, two desc limit 5;

drop table destinTable_temp;

drop table sourceTable_temp;