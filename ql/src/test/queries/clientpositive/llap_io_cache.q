set hive.llap.io.enabled=true;
set hive.llap.io.memory.mode=cache;
set hive.llap.io.allocator.alloc.max=16Mb;
set hive.vectorized.execution.enabled=true;

DROP TABLE IF EXISTS tbl_parq;

CREATE TABLE tbl_parq (
  id INT,
  payload STRING
)
STORED AS PARQUET
TBLPROPERTIES (
  'parquet.block.size'='16777216',
  'parquet.page.size'='16777216',
  'parquet.compression'='UNCOMPRESSED'
);

INSERT INTO TABLE tbl_parq
SELECT
  1 AS id,
  RPAD('x', 16777177, 'x') AS payload;

SELECT LENGTH(payload) FROM tbl_parq;

SELECT SUM(LENGTH(payload)) FROM tbl_parq;