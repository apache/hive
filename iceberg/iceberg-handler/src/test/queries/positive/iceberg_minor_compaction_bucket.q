-- Mask neededVirtualColumns due to non-strict order
--! qt:replace:/(\s+neededVirtualColumns:\s)(.*)/$1#Masked#/
-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
-- Mask a random snapshot id
--! qt:replace:/(\s+current-snapshot-id\s+)\S+(\s*)/$1#Masked#/
-- Mask added file size
--! qt:replace:/(\S\"added-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask total file size
--! qt:replace:/(\S\"total-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/(\s+current-snapshot-timestamp-ms\s+)\S+(\s*)/$1#Masked#$2/
--! qt:replace:/(MINOR\s+succeeded\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/
--! qt:replace:/(MINOR\s+refused\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/
-- Mask compaction id as they will be allocated in parallel threads
--! qt:replace:/^[0-9]/#Masked#/
-- Mask removed file size
--! qt:replace:/(\S\"removed-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask iceberg version
--! qt:replace:/(\S\"iceberg-version\\\":\\\")(\w+\s\w+\s\d+\.\d+\.\d+\s\(\w+\s\w+\))(\\\")/$1#Masked#$3/

set hive.explain.user=true;
set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=false;
set hive.convert.join.bucket.mapjoin.tez=true;

CREATE TABLE srcbucket_big(id string, key int, value string)
PARTITIONED BY SPEC(bucket(4, key)) STORED BY ICEBERG
TBLPROPERTIES ('compactor.threshold.min.input.files'='1');

INSERT INTO srcbucket_big VALUES
('a', 101, 'val_101'),
('b', null, 'val_102'),
('c', 103, 'val_103'),
('d', 104, null),
('e', 105, 'val_105'),
('f', null, null);

ALTER TABLE srcbucket_big SET PARTITION SPEC (bucket(8, key));

INSERT INTO srcbucket_big VALUES
('g', 101, 'val_101'),
('h', null, 'val_102'),
('i', 103, 'val_103'),
('j', 104, null),
('k', 105, 'val_105'),
('l', null, null);

desc formatted default.srcbucket_big;
SELECT * FROM default.srcbucket_big ORDER BY id;

select `partition`, spec_id, record_count
from default.srcbucket_big.partitions
order by `partition`, spec_id, record_count;

alter table srcbucket_big compact 'minor' and wait;
show compactions order by 'partition';

desc formatted default.srcbucket_big;
SELECT * FROM default.srcbucket_big ORDER BY id;

select `partition`, spec_id, record_count
from default.srcbucket_big.partitions
order by `partition`, spec_id, record_count;
