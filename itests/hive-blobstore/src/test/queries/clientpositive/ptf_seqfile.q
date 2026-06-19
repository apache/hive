-- Test several window functions on a table stored using sequence files
DROP TABLE part_seq;
CREATE TABLE part_seq( 
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
STORED AS SEQUENCEFILE 
LOCATION '${hiveconf:test.blobstore.path.unique}/ptf_seqfile/part_seq';

LOAD DATA LOCAL INPATH '../../data/files/part.seq' OVERWRITE INTO TABLE part_seq;

-- Test windowing PTFs with several partitions, using sequence files storage 
SELECT 
  p_mfgr, p_name, p_size,
  RANK() OVER (PARTITION BY p_mfgr ORDER BY p_name) AS r,
  DENSE_RANK() OVER (PARTITION BY p_mfgr ORDER BY p_name) AS dr, 
  SUM(p_retailprice) OVER (
    PARTITION BY p_mfgr 
    ORDER BY p_name ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS s1
FROM NOOP(
  ON part_seq 
  PARTITION BY p_mfgr 
  ORDER BY p_name
);
