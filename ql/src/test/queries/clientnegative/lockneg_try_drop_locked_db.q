set hive.lock.numretries=0;
set hive.support.concurrency=true;

create database lockneg9;

lock database lockneg9 shared;
show locks database lockneg9;

drop database lockneg9;
