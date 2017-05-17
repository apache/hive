-- Test several window functions on a table stored using ORC
DROP TABLE part_orc;
CREATE TABLE part_orc(
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
STORED AS ORC
LOCATION '${hiveconf:test.blobstore.path.unique}/ptf_orcfile/part_orc';

LOAD DATA LOCAL INPATH '../../data/files/part.orc' OVERWRITE INTO TABLE part_orc;

-- Test windowing PTFs with several partitions, using ORC storage 
SELECT 
  p_mfgr, p_name, p_size,
  RANK() OVER (PARTITION BY p_mfgr ORDER BY p_name) AS r,
  DENSE_RANK() OVER (PARTITION BY p_mfgr ORDER BY p_name) AS dr, 
  SUM(p_retailprice) OVER (
    PARTITION BY p_mfgr 
    ORDER BY p_name ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS s1
FROM NOOP(
  ON part_orc 
  PARTITION BY p_mfgr 
  ORDER BY p_name
);
