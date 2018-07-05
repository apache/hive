SET hive.vectorized.execution.enabled=false;
SET hive.explain.user=false;
SET hive.ctas.external.tables=true;
SET hive.external.table.purge.default = true;
--SET hive.execution.mode=llap;

DROP TABLE druid_table_with_nulls;

CREATE EXTERNAL TABLE druid_table_with_nulls
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "HOUR")
AS
SELECT cast(current_timestamp() AS timestamp with local time zone) AS `__time`,
       cast(username AS string) AS username,
       cast(double1 AS double) AS double1,
       cast(int1 AS int) AS int1
FROM TABLE (
  VALUES
    ('alfred', 10.30, 2),
    ('bob', 3.14, null),
    ('bonnie', null, 3),
    ('calvin', null, null),
    ('charlie', 9.8, 1),
    ('charlie', 15.8, 1)) as q (username, double1, int1);

EXPLAIN SELECT
username AS `username`,
SUM(double1) AS `sum_double1`
FROM
druid_table_with_nulls `tbl1`
  JOIN (
    SELECT
    username AS `username`,
    SUM(double1) AS `sum_double2`
    FROM druid_table_with_nulls
    GROUP BY `username`
    ORDER BY `sum_double2`
    DESC  LIMIT 10
  )
  `tbl2`
    ON (`tbl1`.`username` = `tbl2`.`username`)
GROUP BY `tbl1`.`username`;


SELECT
username AS `username`,
SUM(double1) AS `sum_double1`
FROM
druid_table_with_nulls `tbl1`
  JOIN (
    SELECT
    username AS `username`,
    SUM(double1) AS `sum_double2`
    FROM druid_table_with_nulls
    GROUP BY `username`
    ORDER BY `sum_double2`
    DESC  LIMIT 10
  )
  `tbl2`
    ON (`tbl1`.`username` = `tbl2`.`username`)
GROUP BY `tbl1`.`username`;