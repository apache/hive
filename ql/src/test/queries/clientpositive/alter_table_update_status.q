create table src_stat as select * from src1;

create table src_stat_int (
  key         double,
  value       string
);

LOAD DATA LOCAL INPATH '../../data/files/kv3.txt' INTO TABLE src_stat_int;

set hive.stats.dbclass=jdbc:derby;

ANALYZE TABLE src_stat COMPUTE STATISTICS for columns key;

describe formatted src_stat.key;

ALTER TABLE src_stat UPDATE STATISTICS for column key SET ('numDVs'='1111','avgColLen'='1.111');

describe formatted src_stat.key; 

ALTER TABLE src_stat UPDATE STATISTICS for column value SET ('numDVs'='121','numNulls'='122','avgColLen'='1.23','maxColLen'='124');

describe formatted src_stat.value;

ANALYZE TABLE src_stat_int COMPUTE STATISTICS for columns key;

describe formatted src_stat_int.key;

ALTER TABLE src_stat_int UPDATE STATISTICS for column key SET ('numDVs'='2222','lowValue'='333.22','highValue'='22.22');

describe formatted src_stat_int.key; 



create database if not exists dummydb;

use dummydb;

ALTER TABLE default.src_stat UPDATE STATISTICS for column key SET ('numDVs'='3333','avgColLen'='2.222');

describe formatted default.src_stat key;

ALTER TABLE default.src_stat UPDATE STATISTICS for column value SET ('numDVs'='232','numNulls'='233','avgColLen'='2.34','maxColLen'='235');

describe formatted default.src_stat value;

use default;

drop database dummydb;