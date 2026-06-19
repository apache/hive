-- Mask the enqueue time which is based on current time
--! qt:replace:/(initiated\s+---\s+---\s+)[0-9]*(\s+---)/$1#Masked#$2/
--! qt:replace:/(---\s+)[a-zA-Z0-9\-]+(\s+manual)/$1#Masked#$2/
-- Mask the hostname in show compaction
--! qt:replace:/(---\s+)[\S]*(\s+manual)/$1#Masked#$2/
-- Mask compaction id as they will be allocated in parallel threads
--! qt:replace:/^[0-9]/#Masked#/

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table UN_PARTITIONED_T(key string, val string) clustered by (val) into 2 buckets stored as ORC TBLPROPERTIES ('transactional'='true');

create table UN_PARTITIONED_T_MINOR(key string, val string) clustered by (val) into 2 buckets stored as ORC TBLPROPERTIES ('transactional'='true');

create table PARTITIONED_T(key string, val string) partitioned by (dt string) clustered by (val) into 2 buckets stored as ORC TBLPROPERTIES ('transactional'='true');

alter table UN_PARTITIONED_T compact 'major';

alter table UN_PARTITIONED_T_MINOR compact 'minor';

alter table PARTITIONED_T add partition(dt='2023');

insert into PARTITIONED_T partition(dt='2023') values ('k1','v1');
insert into PARTITIONED_T partition(dt='2023') values ('k2','v2');
insert into PARTITIONED_T partition(dt='2023') values ('k3','v3');

alter table PARTITIONED_T partition(dt='2023') compact 'minor';

SHOW COMPACTIONS ORDER BY 'PARTITION' DESC;

alter table PARTITIONED_T add partition(dt='2024');

insert into PARTITIONED_T partition(dt='2024') values ('k1','v1');
insert into PARTITIONED_T partition(dt='2024') values ('k2','v2');
insert into PARTITIONED_T partition(dt='2024') values ('k3','v3');
insert into PARTITIONED_T partition(dt='2024') values ('k4','v4');
insert into PARTITIONED_T partition(dt='2024') values ('k5','v5');
insert into PARTITIONED_T partition(dt='2024') values ('k6','v6');
insert into PARTITIONED_T partition(dt='2024') values ('k7','v7');
insert into PARTITIONED_T partition(dt='2024') values ('k8','v8');
insert into PARTITIONED_T partition(dt='2024') values ('k9','v9');
insert into PARTITIONED_T partition(dt='2024') values ('k10','v10');
insert into PARTITIONED_T partition(dt='2024') values ('k11','v11');

insert into PARTITIONED_T partition(dt='2022') values ('k1','v1');
insert into PARTITIONED_T partition(dt='2022') values ('k2','v2');
insert into PARTITIONED_T partition(dt='2022') values ('k3','v3');
insert into PARTITIONED_T partition(dt='2022') values ('k4','v4');
insert into PARTITIONED_T partition(dt='2022') values ('k5','v5');
insert into PARTITIONED_T partition(dt='2022') values ('k6','v6');
insert into PARTITIONED_T partition(dt='2022') values ('k7','v7');
insert into PARTITIONED_T partition(dt='2022') values ('k8','v8');
insert into PARTITIONED_T partition(dt='2022') values ('k9','v9');
insert into PARTITIONED_T partition(dt='2022') values ('k10','v10');
insert into PARTITIONED_T partition(dt='2022') values ('k11','v11');

explain alter table PARTITIONED_T compact 'major';

alter table PARTITIONED_T compact 'major';

SHOW COMPACTIONS ORDER BY 'PARTITION' DESC;

drop table UN_PARTITIONED_T;

drop table UN_PARTITIONED_T_MINOR;

drop table PARTITIONED_T;
