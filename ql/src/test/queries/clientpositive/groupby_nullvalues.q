set hive.cbo.enable=false;
drop table if exists paqtest;
CREATE TABLE paqtest(
c1 int,
s1 string,
s2 string,
bn1 bigint)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';

insert into paqtest values (58, '', 'ABC', 0);

SELECT
'PM' AS cy,
c1,
NULL AS iused,
NULL AS itp,
s2,
NULL AS cvg,
NULL AS acavg,
sum(bn1) AS cca
FROM paqtest
WHERE (s1 IS NULL OR length(s1) = 0)
GROUP BY 'Pricing mismatch', c1, NULL, NULL, s2, NULL, NULL;
drop table paqtest;
