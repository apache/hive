set hive.vectorized.execution.enabled=false;

drop table if exists test_metadata;
create external table test_metadata (id int, value string) stored by iceberg stored as orc;
insert into test_metadata values (1, 'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five');

select * from default.test_metadata.his;