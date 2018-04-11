set hive.cbo.enable=false;
set hive.mapred.mode=nonstrict;
set hive.map.aggr=true;
set hive.groupby.skewindata=true;
set mapred.reduce.tasks=31;

CREATE TABLE dest1(c1 DOUBLE, c2 DOUBLE, c3 DOUBLE, c4 DOUBLE, c5 DOUBLE, c6 DOUBLE, c7 DOUBLE, c8 DOUBLE, c9 DOUBLE) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT
  sum(substr(src.value,5)),
  avg(substr(src.value,5)),
  avg(DISTINCT substr(src.value,5)),
  max(substr(src.value,5)),
  min(substr(src.value,5)),
  std(substr(src.value,5)),
  stddev_samp(substr(src.value,5)),
  variance(substr(src.value,5)),
  var_samp(substr(src.value,5));

FROM src
INSERT OVERWRITE TABLE dest1 SELECT
  sum(substr(src.value,5)),
  avg(substr(src.value,5)),
  avg(DISTINCT substr(src.value,5)),
  max(substr(src.value,5)),
  min(substr(src.value,5)),
  std(substr(src.value,5)),
  stddev_samp(substr(src.value,5)),
  variance(substr(src.value,5)),
  var_samp(substr(src.value,5));

SELECT ROUND(c1, 1), ROUND(c2, 3), ROUND(c3, 5), ROUND(c4, 1), ROUND(c5, 1), ROUND(c6, 5),
ROUND(c7,5), ROUND(c8, 5), ROUND(c9, 5) FROM dest1;

