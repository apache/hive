-- SORT_QUERY_RESULTS
-- Mask the file size values as it can have slight variability, causing test flakiness
--! qt:replace:/("file_size_in_bytes":)\d+/$1#Masked#/
--! qt:replace:/("total-files-size":)\d+/$1#Masked#/
--! qt:replace:/((ORC|PARQUET|AVRO)\s+\d+\s+)\d+/$1#Masked#/

set tez.mrreader.config.update.properties=hive.io.file.readcolumn.names,hive.io.file.readcolumn.ids;
set hive.query.results.cache.enabled=false;
set hive.fetch.task.conversion=none;
set hive.cbo.enable=true;

drop table if exists ice_meta_2;
drop table if exists ice_meta_3;

create external table ice_meta_2(a int) partitioned by (b string) stored by iceberg stored as orc;
insert into table ice_meta_2 values (1, 'one'), (2, 'one'), (3, 'one');
truncate table ice_meta_2;
insert into table ice_meta_2 values (4, 'two'), (5, 'two');
truncate table ice_meta_2;
insert into table ice_meta_2 values (6, 'three'), (7, 'three'), (8, 'three');
insert into table ice_meta_2 values (9, 'four');
select * from ice_meta_2;


create external table ice_meta_3(a int) partitioned by (b string, c string) stored as orc;
insert into table ice_meta_3 partition (b='one', c='Monday') values (1), (2), (3);
insert into table ice_meta_3 partition (b='two', c='Tuesday') values (4), (5);
insert into table ice_meta_3 partition (b='two', c='Friday') values (10), (11);
insert into table ice_meta_3 partition (b='three', c='Wednesday') values (6), (7), (8);
insert into table ice_meta_3 partition (b='four', c='Thursday') values (9);
insert into table ice_meta_3 partition (b='four', c='Saturday') values (12), (13), (14);
insert into table ice_meta_3 partition (b='four', c='Sunday') values (15);
alter table ice_meta_3 convert to iceberg;
select * from ice_meta_3;


select `partition` from default.ice_meta_2.files;
select `partition` from default.ice_meta_3.files;
select `partition`.b from default.ice_meta_2.files;
select data_file.`partition` from default.ice_meta_3.entries;
select data_file.`partition` from default.ice_meta_2.entries;
select data_file.`partition`.c from default.ice_meta_3.entries;
select summary from default.ice_meta_3.snapshots;
select summary['changed-partition-count'] from default.ice_meta_2.snapshots;
select partition_spec_id, partition_summaries from default.ice_meta_2.manifests;
select partition_spec_id, partition_summaries[1].upper_bound from default.ice_meta_3.manifests;
select `partition` from default.ice_meta_2.partitions;
select `partition` from default.ice_meta_3.partitions;
select `partition` from default.ice_meta_2.partitions where `partition`.b='four';
select `partition` from default.ice_meta_3.partitions where `partition`.b='two' and `partition`.c='Tuesday';
select partition_summaries from default.ice_meta_3.manifests where partition_summaries[1].upper_bound='Wednesday';
select file_format, spec_id from default.ice_meta_2.data_files;
select file_format, spec_id from default.ice_meta_3.data_files;
select content, upper_bounds from default.ice_meta_2.delete_files;
select content, upper_bounds from default.ice_meta_3.delete_files;
select file from default.ice_meta_2.metadata_log_entries;
select file from default.ice_meta_3.metadata_log_entries;
select name, type from default.ice_meta_2.refs;
select name, type from default.ice_meta_3.refs;
select content, file_format from default.ice_meta_2.all_delete_files;
select content, file_format from default.ice_meta_3.all_delete_files;
select file_format, value_counts from default.ice_meta_2.all_files;
select file_format, value_counts from default.ice_meta_3.all_files;


set hive.fetch.task.conversion=more;

select `partition` from default.ice_meta_2.files;
select `partition` from default.ice_meta_3.files;
select `partition`.b from default.ice_meta_2.files;
select data_file.`partition` from default.ice_meta_3.entries;
select data_file.`partition` from default.ice_meta_2.entries;
select data_file.`partition`.c from default.ice_meta_3.entries;
select summary from default.ice_meta_3.snapshots;
select summary['changed-partition-count'] from default.ice_meta_2.snapshots;
select partition_spec_id, partition_summaries from default.ice_meta_2.manifests;
select partition_spec_id, partition_summaries[1].upper_bound from default.ice_meta_3.manifests;
select `partition` from default.ice_meta_2.partitions;
select `partition` from default.ice_meta_3.partitions;
select `partition` from default.ice_meta_2.partitions where `partition`.b='four';
select `partition` from default.ice_meta_3.partitions where `partition`.b='two' and `partition`.c='Tuesday';
select partition_summaries from default.ice_meta_3.manifests where partition_summaries[1].upper_bound='Wednesday';
select file_format, spec_id from default.ice_meta_2.data_files;
select file_format, spec_id from default.ice_meta_3.data_files;
select content, upper_bounds from default.ice_meta_2.delete_files;
select content, upper_bounds from default.ice_meta_3.delete_files;
select file from default.ice_meta_2.metadata_log_entries;
select file from default.ice_meta_3.metadata_log_entries;
select name, type from default.ice_meta_2.refs;
select name, type from default.ice_meta_3.refs;
select content, file_format from default.ice_meta_2.all_delete_files;
select content, file_format from default.ice_meta_3.all_delete_files;
select file_format, value_counts from default.ice_meta_2.all_files;
select file_format, value_counts from default.ice_meta_3.all_files;


drop table ice_meta_2;
drop table ice_meta_3;


CREATE EXTERNAL TABLE `partevv`( `id` int, `ts` timestamp, `ts2` timestamp)  STORED BY ICEBERG STORED AS ORC TBLPROPERTIES  ('format-version'='1');

ALTER TABLE partevv SET PARTITION SPEC (id);
INSERT INTO partevv VALUES (1, '2022-04-29 16:32:01', '2022-04-29 16:32:01');
INSERT INTO partevv VALUES (2, '2022-04-29 16:32:02', '2022-04-29 16:32:02');


ALTER TABLE partevv SET PARTITION SPEC (day(ts));
INSERT INTO partevv VALUES (100, '2022-04-29 16:32:03', '2022-04-29 16:32:03');

select `partition` from default.partevv.partitions;