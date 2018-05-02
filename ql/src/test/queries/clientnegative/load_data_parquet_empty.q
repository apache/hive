CREATE TABLE emptyparquet ( i int) PARTITIONED BY (s string) STORED AS PARQUET;

dfs -touchz ${hiveconf:hive.metastore.warehouse.dir}/parquet_empty;
LOAD DATA INPATH '/Users/djaiswal/parquet/000000_0' INTO TABLE emptyparquet PARTITION (s='something');
