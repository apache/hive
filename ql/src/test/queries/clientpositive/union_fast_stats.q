--! qt:dataset:alltypesorc
set hive.stats.dbclass=fs;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.merge.tezfiles=false;

drop table small_alltypesorc1a_n2;
drop table small_alltypesorc2a_n2;
drop table small_alltypesorc3a_n2;
drop table small_alltypesorc4a_n2;
drop table small_alltypesorc_a_n2;

create table small_alltypesorc1a_n2 as select * from alltypesorc where cint is not null and ctinyint is not null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;
create table small_alltypesorc2a_n2 as select * from alltypesorc where cint is null and ctinyint is not null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;
create table small_alltypesorc3a_n2 as select * from alltypesorc where cint is not null and ctinyint is null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;
create table small_alltypesorc4a_n2 as select * from alltypesorc where cint is null and ctinyint is null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;

create table small_alltypesorc_a_n2 stored as orc as select * from
(select * from (select * from small_alltypesorc1a_n2) sq1
 union all
 select * from (select * from small_alltypesorc2a_n2) sq2
 union all
 select * from (select * from small_alltypesorc3a_n2) sq3
 union all
 select * from (select * from small_alltypesorc4a_n2) sq4) q;

desc formatted small_alltypesorc_a_n2;

ANALYZE TABLE small_alltypesorc_a_n2 COMPUTE STATISTICS;

desc formatted small_alltypesorc_a_n2;

insert into table small_alltypesorc_a_n2 select * from small_alltypesorc1a_n2;

desc formatted small_alltypesorc_a_n2;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.tezfiles=true;

drop table small_alltypesorc1a_n2;
drop table small_alltypesorc2a_n2;
drop table small_alltypesorc3a_n2;
drop table small_alltypesorc4a_n2;
drop table small_alltypesorc_a_n2;

create table small_alltypesorc1a_n2 as select * from alltypesorc where cint is not null and ctinyint is not null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;
create table small_alltypesorc2a_n2 as select * from alltypesorc where cint is null and ctinyint is not null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;
create table small_alltypesorc3a_n2 as select * from alltypesorc where cint is not null and ctinyint is null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;
create table small_alltypesorc4a_n2 as select * from alltypesorc where cint is null and ctinyint is null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;

create table small_alltypesorc_a_n2 stored as orc as select * from
(select * from (select * from small_alltypesorc1a_n2) sq1
 union all
 select * from (select * from small_alltypesorc2a_n2) sq2
 union all
 select * from (select * from small_alltypesorc3a_n2) sq3
 union all
 select * from (select * from small_alltypesorc4a_n2) sq4) q;

desc formatted small_alltypesorc_a_n2;

ANALYZE TABLE small_alltypesorc_a_n2 COMPUTE STATISTICS;

desc formatted small_alltypesorc_a_n2;

insert into table small_alltypesorc_a_n2 select * from small_alltypesorc1a_n2;

desc formatted small_alltypesorc_a_n2;
