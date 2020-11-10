set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;

create database if not exists mydb_e10;

use mydb_e10;

create table if not exists f_tab_e10(
      c1 int,
      c2 timestamp,
      c3 decimal(10,2)
)
partitioned by (c3_date string)
stored as orc TBLPROPERTIES ('transactional'='true');

create table if not exists d1_tab_e10(
      c1 int,
      c2 char(16) not null,
      c3 char(10),
      c4 varchar(60),
      c5 char(15),
      c6 char(10),
      c7 varchar(60),
      c8 varchar(30),
      c9 char(2),
      c10 char(10),
      c11 varchar(20),
      c12 decimal(5,2),
      c13 char(20),
      primary key (c1) disable novalidate rely
)
stored as orc TBLPROPERTIES ('transactional'='true');

create table if not exists d2_tab_e10(
      c1 int,
      c2 char(16),
      c3 int,
      c4 char(10),
      c5 char(20),
      c6 char(30),
      c7 char(13),
      c8 char(50),
      c9 date,
      primary key (c1) disable novalidate rely,
      foreign key  (c3) references d1_tab_e10 (c1) disable novalidate rely
)
stored as orc TBLPROPERTIES ('transactional'='true');

create table if not exists d3_tab_e10(
      c1 int,
      c2 char(16),
      c3 date,
      c4 varchar(200),
      c5 char(2),
      c6 int,
      c7 int,
      c8 char(50),
      primary key (c1) disable novalidate rely
)
stored as orc TBLPROPERTIES ('transactional'='true');

create table if not exists d4_tab_e10(
      c1 char(2),
      c2 decimal(7,2)
)
stored as orc TBLPROPERTIES ('transactional'='true');

create table if not exists f2_tab_e10(
      c1 int,
      c2 int,
      c3 char(2),
      c4 date,
      c5 date,
      foreign key  (c1) references d3_tab_e10 (c1) disable novalidate rely,
      foreign key  (c2) references d2_tab_e10 (c1) disable novalidate rely
)
stored as orc TBLPROPERTIES ('transactional'='true');

explain cbo
select c.c1, c.c5, c.c6, ca.c3, ca.c4, ca.c5, ca.c6,
ca.c7, ca.c8, ca.c9, ca.c10, ca.c11, cm.c1, cm.c4, cm.c5, r.c2
from
f2_tab_e10 cm, d2_tab_e10 c, d1_tab_e10 ca, f_tab_e10 mr, d3_tab_e10 m, d4_tab_e10 r
where
c.c1 = cm.c2
and c.c3 = ca.c1
and cm.c1 = m.c1
and m.c5 = r.c1
and m.c6 = 1
and (cm.c5 is null or cm.c5 > date_format(cast('2020-10-30' as date),'yyyy-MM-01'));

drop table f_tab_e10;
drop table f2_tab_e10;
drop table d2_tab_e10;
drop table d4_tab_e10;
drop table d3_tab_e10;
drop table d1_tab_e10;
