CREATE EXTERNAL TABLE `nested_v1` (`_dc_timelabel` TIMESTAMP COMMENT 'from deserializer', `_dc_load_time` TIMESTAMP COMMENT 'from deserializer', `id` BIGINT COMMENT 'from deserializer', `name` STRING COMMENT 'from deserializer', `location` STRING COMMENT 'from deserializer', `primary_contact_user_id` BIGINT COMMENT 'from deserializer', `parent_id` BIGINT COMMENT 'from deserializer', `parent_office_external_id` STRING COMMENT 'from deserializer', `child_ids` STRING COMMENT 'from deserializer', `child_office_external_ids` STRING COMMENT 'from deserializer', `external_id` STRING COMMENT 'from deserializer') ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

LOAD DATA LOCAL INPATH '../../data/files/nested_sample_1.json' INTO TABLE `nested_v1`;

SELECT * FROM nested_v1;
--
-- Inner nesting
--
CREATE EXTERNAL TABLE `nested_v2` (data STRING, messageid STRING, publish_time BIGINT, attributes STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
--
LOAD DATA LOCAL INPATH '../../data/files/nested_sample_2.json' INTO TABLE nested_v2;
--
SELECT * FROM nested_v2;
