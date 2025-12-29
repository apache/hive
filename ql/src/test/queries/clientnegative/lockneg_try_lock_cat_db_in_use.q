set hive.lock.numretries=0;
set hive.support.concurrency=true;

CREATE CATALOG testcat LOCATION '/tmp/testcat' COMMENT 'Hive test catalog';
create database testcat.lockneg9;

lock database testcat.lockneg9 shared;

drop database testcat.lockneg9;
