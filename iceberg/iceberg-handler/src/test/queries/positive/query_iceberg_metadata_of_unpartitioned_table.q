-- SORT_QUERY_RESULTS
-- Mask the file size values as it can have slight variability, causing test flakiness
--! qt:replace:/("file_size_in_bytes":)\d+/$1#Masked#/
--! qt:replace:/("total-files-size":)\d+/$1#Masked#/
--! qt:replace:/((ORC|PARQUET|AVRO)\s+\d+\s+)\d+/$1#Masked#/

set tez.mrreader.config.update.properties=hive.io.file.readcolumn.names,hive.io.file.readcolumn.ids;
set hive.query.results.cache.enabled=false;
set hive.fetch.task.conversion=none;
set hive.cbo.enable=true;

drop table if exists ice_meta_1;
create external table ice_meta_1 (id int, value string) stored by iceberg stored as orc;
insert into ice_meta_1 values (1, 'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five');
truncate table ice_meta_1;
insert into ice_meta_1 values (3,'three'),(4,'four'),(5,'five');
truncate table ice_meta_1;
insert into ice_meta_1 values (1, 'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five');
insert into ice_meta_1 values (6, 'six'), (7, 'seven');
insert into ice_meta_1 values (8, 'eight'), (9, 'nine'), (10, 'ten');
insert into ice_meta_1 values (NULL, 'eleven'), (12, NULL), (13, NULL);
select * from ice_meta_1;

select * from default.ice_meta_1.files;
select status, sequence_number, data_file from default.ice_meta_1.entries;
select added_data_files_count, existing_data_files_count, deleted_data_files_count from default.ice_meta_1.manifests;
select added_data_files_count, existing_data_files_count, deleted_data_files_count from default.ice_meta_1.all_manifests;
select * from default.ice_meta_1.all_data_files;
select status, sequence_number, data_file from default.ice_meta_1.all_entries;

select file_format, null_value_counts from default.ice_meta_1.files;
select null_value_counts[2] from default.ice_meta_1.files;
select lower_bounds[2], upper_bounds[2] from default.ice_meta_1.files;
select data_file.record_count as record_count, data_file.value_counts[2] as value_counts, data_file.null_value_counts[2] as null_value_counts from default.ice_meta_1.entries;
select summary['added-data-files'], summary['added-records'] from default.ice_meta_1.snapshots;
select summary['deleted-data-files'], summary['deleted-records'] from default.ice_meta_1.snapshots;
select file_format, null_value_counts from default.ice_meta_1.all_data_files;
select null_value_counts[1] from default.ice_meta_1.all_data_files;
select lower_bounds[2], upper_bounds[2] from default.ice_meta_1.all_data_files;
select data_file.record_count as record_count, data_file.value_counts[2] as value_counts, data_file.null_value_counts[2] as null_value_counts from default.ice_meta_1.all_entries;

select file_format, spec_id from default.ice_meta_1.data_files;
select content, upper_bounds from default.ice_meta_1.delete_files;
select file from default.ice_meta_1.metadata_log_entries;
select name, type from default.ice_meta_1.refs;
select content, file_format from default.ice_meta_1.all_delete_files;
select file_format, value_counts from default.ice_meta_1.all_files;

set hive.fetch.task.conversion=more;

select * from default.ice_meta_1.files;
select status, sequence_number, data_file from default.ice_meta_1.entries;
select added_data_files_count, existing_data_files_count, deleted_data_files_count from default.ice_meta_1.manifests;
select added_data_files_count, existing_data_files_count, deleted_data_files_count from default.ice_meta_1.all_manifests;
select * from default.ice_meta_1.all_data_files;
select status, sequence_number, data_file from default.ice_meta_1.all_entries;

select file_format, null_value_counts from default.ice_meta_1.files;
select null_value_counts[2] from default.ice_meta_1.files;
select lower_bounds[2], upper_bounds[2] from default.ice_meta_1.files;
select data_file.record_count as record_count, data_file.value_counts[2] as value_counts, data_file.null_value_counts[2] as null_value_counts from default.ice_meta_1.entries;
select summary['added-data-files'], summary['added-records'] from default.ice_meta_1.snapshots;
select summary['deleted-data-files'], summary['deleted-records'] from default.ice_meta_1.snapshots;
select file_format, null_value_counts from default.ice_meta_1.all_data_files;
select null_value_counts[1] from default.ice_meta_1.all_data_files;
select lower_bounds[2], upper_bounds[2] from default.ice_meta_1.all_data_files;
select data_file.record_count as record_count, data_file.value_counts[2] as value_counts, data_file.null_value_counts[2] as null_value_counts from default.ice_meta_1.all_entries;

select file_format, spec_id from default.ice_meta_1.data_files;
select content, upper_bounds from default.ice_meta_1.delete_files;
select file from default.ice_meta_1.metadata_log_entries;
select name, type from default.ice_meta_1.refs;
select content, file_format from default.ice_meta_1.all_delete_files;
select file_format, value_counts from default.ice_meta_1.all_files;

drop table ice_meta_1;
