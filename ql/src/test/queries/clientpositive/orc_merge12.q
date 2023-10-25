--! qt:replace:/(File Version:)(.+)/$1#Masked#/
set hive.vectorized.execution.enabled=false;

CREATE TABLE `alltypesorc3xcols`(
  `atinyint` tinyint,
  `asmallint` smallint,
  `aint` int,
  `abigint` bigint,
  `afloat` float,
  `adouble` double,
  `astring1` string,
  `astring2` string,
  `atimestamp1` timestamp,
  `atimestamp2` timestamp,
  `aboolean1` boolean,
  `aboolean2` boolean,
  `btinyint` tinyint,
  `bsmallint` smallint,
  `bint` int,
  `bbigint` bigint,
  `bfloat` float,
  `bdouble` double,
  `bstring1` string,
  `bstring2` string,
  `btimestamp1` timestamp,
  `btimestamp2` timestamp,
  `bboolean1` boolean,
  `bboolean2` boolean,
  `ctinyint` tinyint,
  `csmallint` smallint,
  `cint` int,
  `cbigint` bigint,
  `cfloat` float,
  `cdouble` double,
  `cstring1` string,
  `cstring2` string,
  `ctimestamp1` timestamp,
  `ctimestamp2` timestamp,
  `cboolean1` boolean,
  `cboolean2` boolean) stored as ORC;

load data local inpath '../../data/files/alltypesorc3xcols' into table alltypesorc3xcols;
load data local inpath '../../data/files/alltypesorc3xcols' into table alltypesorc3xcols;

select count(*) from alltypesorc3xcols;
select sum(hash(*)) from alltypesorc3xcols;

alter table alltypesorc3xcols concatenate;

select count(*) from alltypesorc3xcols;
select sum(hash(*)) from alltypesorc3xcols;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecOrcFileDump;
select * from alltypesorc3xcols limit 1;
