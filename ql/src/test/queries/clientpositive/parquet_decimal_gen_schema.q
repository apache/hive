dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/parquet_decimal_gen_schema;
dfs -cp ${system:hive.root}/data/files/dec.parq ${system:test.tmp.dir}/parquet_decimal_gen_schema;

CREATE TABLE parquet_decimal_gen_schema
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${system:test.tmp.dir}/parquet_decimal_gen_schema';

DESCRIBE FORMATTED parquet_decimal_gen_schema;

SELECT * FROM parquet_decimal_gen_schema;

DROP TABLE parquet_decimal_gen_schema;