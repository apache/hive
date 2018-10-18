--! qt:dataset:srcpart
--! qt:dataset:src
create database statsdb;
use statsdb;
create table srctable like default.src;
load data local inpath '../../data/files/kv1.txt' overwrite into table srctable;

analyze table srctable compute statistics;
describe formatted srctable;

alter table srctable touch;
alter table srctable rename to statstable;

alter table statstable add columns (newcol string);
alter table statstable change key key string;
alter table statstable set tblproperties('testtblstats'='unchange');
describe formatted statstable;

alter table statstable update statistics set ('numRows' = '1000');
describe formatted statstable;

analyze table statstable compute statistics;
describe formatted statstable;
alter table statstable set location '${system:test.tmp.dir}/newdir';
describe formatted statstable;

drop table statstable;

create table srcpart_n0 like default.srcpart;
load data local inpath '../../data/files/kv1.txt' overwrite into table srcpart_n0 partition (ds='2008-04-08', hr='11');
load data local inpath '../../data/files/kv1.txt' overwrite into table srcpart_n0 partition (ds='2008-04-08', hr='12');

analyze table srcpart_n0 partition (ds='2008-04-08', hr='11') compute statistics;
describe formatted srcpart_n0 partition (ds='2008-04-08', hr='11');

alter table srcpart_n0 touch;
alter table srcpart_n0 partition (ds='2008-04-08', hr='11') rename to partition (ds='2017-01-19', hr='11');
alter table srcpart_n0 partition (ds='2017-01-19', hr='11') add columns (newcol string);
alter table srcpart_n0 partition (ds='2017-01-19', hr='11') change key key string;
alter table srcpart_n0 set tblproperties('testpartstats'='unchange');
describe formatted srcpart_n0 partition (ds='2017-01-19', hr='11');

alter table srcpart_n0 partition (ds='2017-01-19', hr='11') update statistics set ('numRows' = '1000');
describe formatted srcpart_n0 partition (ds='2017-01-19', hr='11');

analyze table srcpart_n0 partition (ds='2017-01-19', hr='11') compute statistics;
describe formatted srcpart_n0 partition (ds='2017-01-19', hr='11');

drop table srcpart_n0;

