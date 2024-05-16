-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
-- Mask a random snapshot id
--! qt:replace:/(\s+current-snapshot-id\s+)\S+(\s*)/$1#Masked#/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/(\s+current-snapshot-timestamp-ms\s+)\S+(\s*)/$1#Masked#$2/
-- Mask added file size
--! qt:replace:/(\S\"added-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask total file size
--! qt:replace:/(\S\"total-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask show compactions fields that change across runs
--! qt:replace:/(MAJOR\s+succeeded\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/

set hive.llap.io.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.optimize.shared.work.merge.ts.schema=true;
set iceberg.mr.schema.auto.conversion=true;

CREATE TABLE x (name VARCHAR(50), age TINYINT, num_clicks BIGINT) 
stored by iceberg stored as orc 
TBLPROPERTIES ('external.table.purge'='true','format-version'='2');

insert into x values 
('amy', 35, 123412344),
('adxfvy', 36, 123412534),
('amsdfyy', 37, 123417234),
('asafmy', 38, 123412534);

insert into x values 
('amerqwy', 39, 123441234),
('amyxzcv', 40, 123341234),
('erweramy', 45, 122341234);

select * from default.x.data_files;
select count(*) from default.x.data_files;

alter table x compact 'major' and wait;

show compactions;
desc formatted x;

select * from default.x.data_files;
select count(*) from default.x.data_files;
