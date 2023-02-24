-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
-- Mask random snapshot id
--! qt:replace:/('current-snapshot-id'=')\d+/$1#SnapshotId#/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/('current-snapshot-timestamp-ms'=')\d+/$1#Masked#/

DROP TABLE IF EXISTS ice_t;
CREATE EXTERNAL TABLE ice_t (i int, s string, ts timestamp, d date) STORED BY ICEBERG;
SHOW CREATE TABLE ice_t;

DROP TABLE IF EXISTS ice_t_transform;
CREATE EXTERNAL TABLE ice_t_transform (year_field date, month_field date, day_field date, hour_field timestamp, truncate_field string, bucket_field int, identity_field int) PARTITIONED BY SPEC (year(year_field), month(month_field), day(day_field), hour(hour_field), truncate(2, truncate_field), bucket(2, bucket_field), identity_field) STORED BY ICEBERG;
SHOW CREATE TABLE ice_t_transform;

DROP TABLE IF EXISTS ice_t_transform_prop;
CREATE EXTERNAL TABLE ice_t_transform_prop (id int, year_field date, month_field date, day_field date, hour_field timestamp, truncate_field string, bucket_field int, identity_field int) STORED BY ICEBERG TBLPROPERTIES ('iceberg.mr.table.partition.spec'='{"spec-id":0,"fields":[{"name":"year_field_year","transform":"year","source-id":2,"field-id":1000},{"name":"month_field_month","transform":"month","source-id":3,"field-id":1001},{"name":"day_field_day","transform":"day","source-id":4,"field-id":1002},{"name":"hour_field_hour","transform":"hour","source-id":5,"field-id":1003},{"name":"truncate_field_trunc","transform":"truncate[2]","source-id":6,"field-id":1004},{"name":"bucket_field_bucket","transform":"bucket[2]","source-id":7,"field-id":1005},{"name":"identity_field","transform":"identity","source-id":8,"field-id":1006}]}');
SHOW CREATE TABLE ice_t_transform_prop;

DROP TABLE IF EXISTS ice_t_identity_part;
CREATE EXTERNAL TABLE ice_t_identity_part (a int) PARTITIONED BY (b string) STORED BY ICEBERG;
SHOW CREATE TABLE ice_t_identity_part;

DROP TABLE IF EXISTS ice_data;
CREATE EXTERNAL TABLE ice_data (i int, s string) STORED BY ICEBERG;
INSERT INTO ice_data VALUES (1, 'ABC'),(2, 'CCC'),(3, 'DBD');
SHOW CREATE TABLE ice_data;
