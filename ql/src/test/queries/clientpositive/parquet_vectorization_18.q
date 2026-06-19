dfs ${system:test.dfs.mkdir} -p ${system:test.tmp.dir}/hive22670;
dfs -copyFromLocal ../../data/files/hive22670.parquet ${system:test.tmp.dir}/hive22670/;
dfs -ls ${system:test.tmp.dir}/hive22670/;

drop table if exists test_parquet_na;
create external table test_parquet_na(
            x int,
            y int)
  stored as parquet
  location '${system:test.tmp.dir}/hive22670';

set hive.vectorized.execution.enabled=false;
select * from test_parquet_na;
select * from test_parquet_na order by y;

set hive.vectorized.execution.enabled=true;
select * from test_parquet_na;

set hive.vectorized.execution.enabled=true;
select * from test_parquet_na order by y;

drop table test_parquet_na;
dfs -ls  ${system:test.tmp.dir}/hive22670/;
dfs -rmr  ${system:test.tmp.dir}/hive22670;
