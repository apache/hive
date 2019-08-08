dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/parquet_decimal_gen_schema_ext_tmp;
dfs -cp ${system:hive.root}/data/files/HiveRequiredGroupInList.parquet ${system:test.tmp.dir}/parquet_decimal_gen_schema_ext_tmp;

CREATE EXTERNAL TABLE parquet_array_of_structs_gen_schema_ext
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${system:test.tmp.dir}/parquet_decimal_gen_schema_ext_tmp';

SELECT * FROM parquet_array_of_structs_gen_schema_ext;

DROP TABLE parquet_array_of_structs_gen_schema_ext;
