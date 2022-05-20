-- SORT_QUERY_RESULTS
-- Mask the file size values as it can have slight variability, causing test flakiness
--! qt:replace:/("file_size_in_bytes":)\d+/$1#Masked#/
--! qt:replace:/("total-files-size":)\d+/$1#Masked#/

set tez.mrreader.config.update.properties=hive.io.file.readcolumn.names,hive.io.file.readcolumn.ids;
set hive.query.results.cache.enabled=false;
set hive.fetch.task.conversion=none;
set hive.cbo.enable=true;

drop table if exists ice_meta_2;
create external table ice_meta_2 (id int, value string) stored by iceberg stored as orc;
insert into ice_meta_2 values (1, 'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five');
truncate table ice_meta_2;
insert into ice_meta_2 values (3,'three'),(4,'four'),(5,'five');
truncate table ice_meta_2;
insert into ice_meta_2 values (1, 'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five');
insert into ice_meta_2 values (6, 'six'), (7, 'seven');
insert into ice_meta_2 values (8, 'eight'), (9, 'nine'), (10, 'ten');
insert into ice_meta_2 values (NULL, 'eleven'), (12, NULL), (13, NULL);
select * from ice_meta_2;

select
    s.operation,
    h.is_current_ancestor,
    s.summary['added-records'] as added_records,
    s.summary['deleted-records'] as deleted_records
from default.ice_meta_2.history h
join default.ice_meta_2.snapshots s
  on h.snapshot_id = s.snapshot_id
order by s.operation, s.summary['added-records'];


select
  s.operation,
    h.is_current_ancestor,
    s.summary['added-records'] as added_records,
    m.added_data_files_count,
    e.data_file.content,
    e.data_file.file_format,
    e.data_file.record_count,
    e.status
from default.ice_meta_2.history h
join default.ice_meta_2.snapshots s
  on h.snapshot_id = s.snapshot_id
join default.ice_meta_2.entries e
  on e.snapshot_id = h.snapshot_id
join default.ice_meta_2.manifests m
  on m.added_snapshot_id = h.snapshot_id
where s.summary['added-records'] > 2
order by s.operation, record_count;


set hive.cbo.enable=false;

select
    s.operation,
    h.is_current_ancestor,
    s.summary['added-records'] as added_records,
    s.summary['deleted-records'] as deleted_records
from default.ice_meta_2.history h
join default.ice_meta_2.snapshots s
  on h.snapshot_id = s.snapshot_id
order by s.operation, added_records;


select
  s.operation,
    h.is_current_ancestor,
    s.summary['added-records'] as added_records,
    m.added_data_files_count,
    e.data_file.content,
    e.data_file.file_format,
    e.data_file.record_count,
    e.status
from default.ice_meta_2.history h
join default.ice_meta_2.snapshots s
  on h.snapshot_id = s.snapshot_id
join default.ice_meta_2.entries e
  on e.snapshot_id = h.snapshot_id
join default.ice_meta_2.manifests m
  on m.added_snapshot_id = h.snapshot_id
where s.summary['added-records'] > 2
order by s.operation, record_count;

drop table ice_meta_2;
