set hive.mapred.mode=nonstrict;
set hive.map.aggr=true;
set hive.groupby.skewindata=false;
set mapred.reduce.tasks=31;

CREATE TABLE dest1_n53(c1 DOUBLE, c2 DOUBLE, c3 DOUBLE, c4 DOUBLE, c5 DOUBLE, c6 DOUBLE, c7 DOUBLE, c8 DOUBLE, c9 DOUBLE) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1_n53 SELECT
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
INSERT OVERWRITE TABLE dest1_n53 SELECT
  sum(substr(src.value,5)),
  avg(substr(src.value,5)),
  avg(DISTINCT substr(src.value,5)),
  max(substr(src.value,5)),
  min(substr(src.value,5)),
  std(substr(src.value,5)),
  stddev_samp(substr(src.value,5)),
  variance(substr(src.value,5)),
  var_samp(substr(src.value,5));

SELECT
c1,
c2,
round(c3, 11) c3,
c4,
c5,
round(c6, 11) c6,
round(c7, 11) c7,
round(c8, 5) c8,
round(c9, 9) c9
FROM dest1_n53;


