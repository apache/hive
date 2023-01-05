set hive.query.lifetime.hooks=org.apache.iceberg.mr.hive.HiveIcebergQueryLifeTimeHook;
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
-- Mask a random snapshot id
--! qt:replace:/(\s+current-snapshot-id\s+)\S+(\s*)/$1#Masked#/
-- Mask added file size
--! qt:replace:/(\S\"added-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask total file size
--! qt:replace:/(\S\"total-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/(\s+current-snapshot-timestamp-ms\s+)\S+(\s*)/$1#Masked#$2/

set hive.explain.user=false;

create table source(a int, b string, c int);

insert into source values (1, 'one', 3);
insert into source values (1, 'two', 4);

explain extended
create external table tbl_ice partitioned by spec (bucket(16, a), truncate(3, b)) stored by iceberg stored as orc tblproperties ('format-version'='2') as
select a, b, c from source;

create external table tbl_ice partitioned by spec (bucket(16, a), truncate(3, b)) stored by iceberg stored as orc tblproperties ('format-version'='2') as
select a, b, c from source;

describe formatted tbl_ice;

select * from tbl_ice;
