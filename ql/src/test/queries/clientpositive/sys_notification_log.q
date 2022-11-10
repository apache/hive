--! qt:sysdb
set hive.metastore.event.listeners=org.apache.hive.hcatalog.listener.DbNotificationListener;

drop database if exists srcnotification cascade;

create database srcnotification;

use srcnotification;

create table emp01 (id int);

create table emp02 (id int);

select count(*) from sys.notification_log where db_name='srcnotification' AND event_type='CREATE_TABLE';

drop table emp02;

select tbl_name from sys.notification_log where db_name='srcnotification' AND event_type='DROP_TABLE';