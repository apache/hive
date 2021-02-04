set hive.lock.numretries=0;
set hive.support.concurrency=true;

create database lockneg4;

lock database lockneg4 exclusive;
lock database lockneg4 shared;
