-- Check behavior of MATCHPATH with a partitioned table with several partitions, 
-- and with a partitioned table with a single partition
DROP TABLE flights_tiny;
CREATE TABLE flights_tiny (
  origin_city_name string,
  dest_city_name string,
  year int,
  month int,
  day_of_month int,
  arr_delay float,
  fl_num string
)
LOCATION '${hiveconf:test.blobstore.path.unique}/ptf_matchpath/flights_tiny';

LOAD DATA LOCAL INPATH '../../data/files/flights_tiny.txt' INTO TABLE flights_tiny;

-- basic MATCHPATH test
SELECT origin_city_name, fl_num, year, month, day_of_month, sz, tpath
FROM MATCHPATH(
  ON flights_tiny 
  DISTRIBUTE BY fl_num
  SORT BY year, month, day_of_month
  ARG1('LATE.LATE+'), ARG2('LATE'), ARG3(arr_delay > 15),
  ARG4('origin_city_name, fl_num, year, month, day_of_month, SIZE(tpath) AS sz, tpath[0].day_of_month AS tpath')
);

-- MATCHPATH on 1 partition
SELECT origin_city_name, fl_num, year, month, day_of_month, sz, tpath
FROM MATCHPATH(
  ON flights_tiny 
  SORT BY year, month, day_of_month, fl_num, origin_city_name
  ARG1('LATE.LATE+'), ARG2('LATE'), ARG3(arr_delay > 15),
  ARG4('origin_city_name, fl_num, year, month, day_of_month, SIZE(tpath) AS sz, tpath[0].day_of_month AS tpath')
)
WHERE fl_num = 1142;
