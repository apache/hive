-- Test several window functions on a table stored using PARQUET
DROP TABLE part_parquet;
CREATE TABLE part_parquet(
  p_partkey int,
  p_name string,
  p_mfgr string,
  p_brand string,
  p_type string,
  p_size int,
  p_container string,
  p_retailprice double,
  p_comment string
)
STORED AS PARQUET
LOCATION '${hiveconf:test.blobstore.path.unique}/ptf_parquetfile/part_parquet';

LOAD DATA LOCAL INPATH '../../data/files/part.parquet' OVERWRITE INTO TABLE part_parquet;

-- Test windowing PTFs with several partitions, using PARQUET storage 
SELECT 
  p_mfgr, p_name, p_size,
  RANK() OVER (PARTITION BY p_mfgr ORDER BY p_name) AS r,
  DENSE_RANK() OVER (PARTITION BY p_mfgr ORDER BY p_name) AS dr, 
  SUM(p_retailprice) OVER (
    PARTITION BY p_mfgr 
    ORDER BY p_name ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS s1
FROM NOOP(
  ON part_parquet 
  PARTITION BY p_mfgr 
  ORDER BY p_name
);
