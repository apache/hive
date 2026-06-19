set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

DROP TABLE IF EXISTS int_txt;

CREATE TABLE int_txt (`i` int);

LOAD DATA LOCAL INPATH '../../data/files/decimal_10_0.txt' OVERWRITE INTO TABLE int_txt;

select count(*)
from
  int_txt
  where
         (( 1.0 * i) / ( 1.0 * i)) > 1.2;

DROP TABLE IF EXISTS int_txt;
