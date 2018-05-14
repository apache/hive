SELECT '< HIVE-6757 Remove deprecated parquet classes from outside of org.apache package >';

UPDATE "SDS"
  SET "INPUT_FORMAT" = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
WHERE
  "INPUT_FORMAT"= 'parquet.hive.DeprecatedParquetInputFormat' or
  "INPUT_FORMAT" = 'parquet.hive.MapredParquetInputFormat'
;

UPDATE "SDS"
  SET "OUTPUT_FORMAT" = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
WHERE
  "OUTPUT_FORMAT" = 'parquet.hive.DeprecatedParquetOutputFormat'  or
  "OUTPUT_FORMAT" = 'parquet.hive.MapredParquetOutputFormat'
;

UPDATE "SERDES"
  SET "SLIB" ='org.apache.hadoop.hive.ql.io.parquet.serdes.ParquetHiveSerDe'
WHERE
  "SLIB" = 'parquet.hive.serdes.ParquetHiveSerDe'
;