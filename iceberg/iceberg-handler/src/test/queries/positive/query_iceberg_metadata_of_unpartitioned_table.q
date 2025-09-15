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
select * from ice_meta_1 order by id;

select * from default.ice_meta_1.files order by record_count, file_path;
select status, sequence_number, data_file from default.ice_meta_1.entries order by status, sequence_number;
select added_data_files_count, existing_data_files_count, deleted_data_files_count from default.ice_meta_1.manifests order by added_data_files_count, existing_data_files_count, deleted_data_files_count;
select added_data_files_count, existing_data_files_count, deleted_data_files_count from default.ice_meta_1.all_manifests order by added_data_files_count, existing_data_files_count, deleted_data_files_count;
select * from default.ice_meta_1.all_data_files order by record_count, file_path;
select status, sequence_number, data_file from default.ice_meta_1.all_entries order by status, sequence_number;

select file_format, null_value_counts from default.ice_meta_1.files order by null_value_counts;
select null_value_counts[2] from default.ice_meta_1.files order by null_value_counts[2];
select lower_bounds[2], upper_bounds[2] from default.ice_meta_1.files order by lower_bounds[2], upper_bounds[2];
select data_file.record_count as record_count, data_file.value_counts[2] as value_counts, data_file.null_value_counts[2] as null_value_counts from default.ice_meta_1.entries order by data_file.record_count, data_file.value_counts[2], data_file.null_value_counts[2];
select summary['added-data-files'], summary['added-records'] from default.ice_meta_1.snapshots order by summary['added-data-files'], summary['added-records'];
select summary['deleted-data-files'], summary['deleted-records'] from default.ice_meta_1.snapshots order by summary['deleted-data-files'], summary['deleted-records'];
select file_format, null_value_counts from default.ice_meta_1.all_data_files order by null_value_counts;
select null_value_counts[1] from default.ice_meta_1.all_data_files order by null_value_counts[1];
select lower_bounds[2], upper_bounds[2] from default.ice_meta_1.all_data_files order by lower_bounds[2], upper_bounds[2];
select data_file.record_count as record_count, data_file.value_counts[2] as value_counts, data_file.null_value_counts[2] as null_value_counts from default.ice_meta_1.all_entries order by data_file.record_count, data_file.value_counts[2], data_file.null_value_counts[2];

select file_format, spec_id from default.ice_meta_1.data_files order by file_format, spec_id;
select content, upper_bounds from default.ice_meta_1.delete_files order by content, upper_bounds;
select file from default.ice_meta_1.metadata_log_entries order by file;
select name, type from default.ice_meta_1.refs order by name, type;
select content, file_format from default.ice_meta_1.all_delete_files order by content, file_format;
select file_format, value_counts from default.ice_meta_1.all_files order by file_format, value_counts;

set hive.fetch.task.conversion=more;

select * from default.ice_meta_1.files order by record_count, file_path;
select status, sequence_number, data_file from default.ice_meta_1.entries order by status, sequence_number;
select added_data_files_count, existing_data_files_count, deleted_data_files_count from default.ice_meta_1.manifests order by added_data_files_count, existing_data_files_count, deleted_data_files_count;
select added_data_files_count, existing_data_files_count, deleted_data_files_count from default.ice_meta_1.all_manifests order by added_data_files_count, existing_data_files_count, deleted_data_files_count;
select * from default.ice_meta_1.all_data_files order by record_count, file_path;
select status, sequence_number, data_file from default.ice_meta_1.all_entries order by status, sequence_number;

select file_format, null_value_counts from default.ice_meta_1.files order by null_value_counts;
select null_value_counts[2] from default.ice_meta_1.files order by null_value_counts[2];
select lower_bounds[2], upper_bounds[2] from default.ice_meta_1.files order by lower_bounds[2], upper_bounds[2];
select data_file.record_count as record_count, data_file.value_counts[2] as value_counts, data_file.null_value_counts[2] as null_value_counts from default.ice_meta_1.entries order by data_file.record_count, data_file.value_counts[2], data_file.null_value_counts[2];
select summary['added-data-files'], summary['added-records'] from default.ice_meta_1.snapshots order by summary['added-data-files'], summary['added-records'];
select summary['deleted-data-files'], summary['deleted-records'] from default.ice_meta_1.snapshots order by summary['deleted-data-files'], summary['deleted-records'];
select file_format, null_value_counts from default.ice_meta_1.all_data_files order by null_value_counts;
select null_value_counts[1] from default.ice_meta_1.all_data_files order by null_value_counts[1];
select lower_bounds[2], upper_bounds[2] from default.ice_meta_1.all_data_files order by lower_bounds[2], upper_bounds[2];
select data_file.record_count as record_count, data_file.value_counts[2] as value_counts, data_file.null_value_counts[2] as null_value_counts from default.ice_meta_1.all_entries order by data_file.record_count, data_file.value_counts[2], data_file.null_value_counts[2];

select file_format, spec_id from default.ice_meta_1.data_files order by file_format, spec_id;
select content, upper_bounds from default.ice_meta_1.delete_files order by content, upper_bounds;
select file from default.ice_meta_1.metadata_log_entries order by file;
select name, type from default.ice_meta_1.refs order by name, type;
select content, file_format from default.ice_meta_1.all_delete_files order by content, file_format;
select file_format, value_counts from default.ice_meta_1.all_files order by file_format, value_counts;

drop table ice_meta_1;
