set hive.stats.dbclass=fs;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.merge.tezfiles=false;

drop table small_alltypesorc1a;
drop table small_alltypesorc2a;
drop table small_alltypesorc3a;
drop table small_alltypesorc4a;
drop table small_alltypesorc_a;

create table small_alltypesorc1a as select * from alltypesorc where cint is not null and ctinyint is not null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;
create table small_alltypesorc2a as select * from alltypesorc where cint is null and ctinyint is not null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;
create table small_alltypesorc3a as select * from alltypesorc where cint is not null and ctinyint is null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;
create table small_alltypesorc4a as select * from alltypesorc where cint is null and ctinyint is null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;

create table small_alltypesorc_a stored as orc as select * from
(select * from (select * from small_alltypesorc1a) sq1
 union all
 select * from (select * from small_alltypesorc2a) sq2
 union all
 select * from (select * from small_alltypesorc3a) sq3
 union all
 select * from (select * from small_alltypesorc4a) sq4) q;

desc formatted small_alltypesorc_a;

ANALYZE TABLE small_alltypesorc_a COMPUTE STATISTICS;

desc formatted small_alltypesorc_a;

insert into table small_alltypesorc_a select * from small_alltypesorc1a;

desc formatted small_alltypesorc_a;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.tezfiles=true;

drop table small_alltypesorc1a;
drop table small_alltypesorc2a;
drop table small_alltypesorc3a;
drop table small_alltypesorc4a;
drop table small_alltypesorc_a;

create table small_alltypesorc1a as select * from alltypesorc where cint is not null and ctinyint is not null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;
create table small_alltypesorc2a as select * from alltypesorc where cint is null and ctinyint is not null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;
create table small_alltypesorc3a as select * from alltypesorc where cint is not null and ctinyint is null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;
create table small_alltypesorc4a as select * from alltypesorc where cint is null and ctinyint is null order by ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 limit 5;

create table small_alltypesorc_a stored as orc as select * from
(select * from (select * from small_alltypesorc1a) sq1
 union all
 select * from (select * from small_alltypesorc2a) sq2
 union all
 select * from (select * from small_alltypesorc3a) sq3
 union all
 select * from (select * from small_alltypesorc4a) sq4) q;

desc formatted small_alltypesorc_a;

ANALYZE TABLE small_alltypesorc_a COMPUTE STATISTICS;

desc formatted small_alltypesorc_a;

insert into table small_alltypesorc_a select * from small_alltypesorc1a;

desc formatted small_alltypesorc_a;
