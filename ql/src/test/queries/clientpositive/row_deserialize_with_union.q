SET hive.vectorized.execution.enabled=true;
set hive.vectorized.use.row.serde.deserialize=true;
set hive.vectorized.use.vector.serde.deserialize=false;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/data_with_union/;
dfs -copyFromLocal ../../data/files/data_with_union.txt ${system:test.tmp.dir}/data_with_union/data_with_union.txt;

CREATE EXTERNAL TABLE data_with_union(
  unionfield uniontype<int, string>,
  arrayfield array<int>,
  mapfield map<int,int>,
  structfield struct<`sf1`:int, `sf2`:string>)
stored as textfile
location '${system:test.tmp.dir}/data_with_union';

create table data_with_union_2 as select * from data_with_union;

select * from data_with_union_2;