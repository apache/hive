CREATE TABLE myparquettable_parted
(
  name string,
  favnumber int,
  favcolor string
)
PARTITIONED BY (day string)
STORED AS PARQUET;

INSERT OVERWRITE TABLE myparquettable_parted
PARTITION(day='2017-04-04')
SELECT
   'mary' as name,
   5 AS favnumber,
   'blue' AS favcolor;

alter table myparquettable_parted
REPLACE COLUMNS
(
name string,
favnumber int
);
