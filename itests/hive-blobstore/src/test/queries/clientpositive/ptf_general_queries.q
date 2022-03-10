-- Check some basic PTF operations
DROP TABLE part_tiny;
CREATE TABLE part_tiny(
  p_partkey INT,
  p_name STRING,
  p_mfgr STRING,
  p_brand STRING,
  p_type STRING,
  p_size INT,
  p_container STRING,
  p_retailprice DOUBLE,
  p_comment STRING
)
LOCATION '${hiveconf:test.blobstore.path.unique}/ptf_general_queries/part_tiny';

LOAD DATA LOCAL INPATH '../../data/files/tpch/tiny/part.tbl.bz2' INTO TABLE part_tiny;

-- Test DISTRIBUTE BY without any aggregate function 
SELECT p_mfgr, p_name, p_size
FROM part_tiny
DISTRIBUTE BY p_mfgr
SORT BY p_name;
        
-- Test using UDAF without windowing nor group by 
SELECT 
  p_mfgr,p_name,p_retailprice,
  SUM(p_retailprice) OVER(PARTITION BY p_mfgr ORDER BY p_name) AS sum,
  MIN(p_retailprice) OVER(PARTITION BY p_mfgr ORDER BY p_name) AS min,
  MAX(p_retailprice) OVER(PARTITION BY p_mfgr ORDER BY p_name) AS max,
  AVG(p_retailprice) OVER(PARTITION BY p_mfgr ORDER BY p_name) AS avg
FROM part_tiny;
        
-- Test using a constant expression in SELECT 
SELECT 'tst1' AS key, COUNT(1) AS value FROM part_tiny;
