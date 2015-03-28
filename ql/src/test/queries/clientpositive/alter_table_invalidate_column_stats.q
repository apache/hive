set hive.metastore.try.direct.sql=true;

drop database if exists statsdb1;
create database statsdb1;
drop database if exists statsdb2;
create database statsdb2;

create table statsdb1.testtable1 (col1 int, col2 string, col3 string);
insert into statsdb1.testtable1 select key, value, 'val3' from src limit 10;

create table statsdb1.testpart1 (col1 int, col2 string, col3 string) partitioned by (part string);
insert into statsdb1.testpart1 partition (part = 'part1') select key, value, 'val3' from src limit 10;
insert into statsdb1.testpart1 partition (part = 'part2') select key, value, 'val3' from src limit 20;

use statsdb1;

analyze table testtable1 compute statistics for columns;

describe formatted statsdb1.testtable1 col1;
describe formatted statsdb1.testtable1 col2;
describe formatted statsdb1.testtable1 col3;

alter table testtable1 replace columns (col1 int, col2 string, col4 string);
describe formatted statsdb1.testtable1 col1;
describe formatted statsdb1.testtable1 col2;
describe formatted statsdb1.testtable1 col4;

alter table testtable1 change col1 col1 string;
describe formatted statsdb1.testtable1 col1;
describe formatted statsdb1.testtable1 col2;
describe formatted statsdb1.testtable1 col4;

alter table statsdb1.testtable1 rename to statsdb2.testtable2;


analyze table testpart1 compute statistics for columns;

describe formatted statsdb1.testpart1 col1 partition (part = 'part1');
describe formatted statsdb1.testpart1 col2 partition (part = 'part1');
describe formatted statsdb1.testpart1 col3 partition (part = 'part1');
describe formatted statsdb1.testpart1 col1 partition (part = 'part2');
describe formatted statsdb1.testpart1 col2 partition (part = 'part2');
describe formatted statsdb1.testpart1 col3 partition (part = 'part2');

alter table statsdb1.testpart1 partition (part = 'part2') rename to partition (part = 'part3');
describe formatted statsdb1.testpart1 col1 partition (part = 'part1');
describe formatted statsdb1.testpart1 col2 partition (part = 'part1');
describe formatted statsdb1.testpart1 col3 partition (part = 'part1');
describe formatted statsdb1.testpart1 col1 partition (part = 'part3');
describe formatted statsdb1.testpart1 col2 partition (part = 'part3');
describe formatted statsdb1.testpart1 col3 partition (part = 'part3');

alter table statsdb1.testpart1 replace columns (col1 int, col2 string, col4 string) cascade;
describe formatted statsdb1.testpart1 col1 partition (part = 'part1');
describe formatted statsdb1.testpart1 col2 partition (part = 'part1');
describe formatted statsdb1.testpart1 col4 partition (part = 'part1');

alter table statsdb1.testpart1 change column col1 col1 string;
set hive.exec.dynamic.partition = true;
alter table statsdb1.testpart1 partition (part) change column col1 col1 string;
describe formatted statsdb1.testpart1 col1 partition (part = 'part1');
describe formatted statsdb1.testpart1 col2 partition (part = 'part1');
describe formatted statsdb1.testpart1 col4 partition (part = 'part1');

alter table statsdb1.testpart1 rename to statsdb2.testpart2;
use statsdb2;

alter table statsdb2.testpart2 drop partition (part = 'part1');
drop table statsdb2.testpart2;

drop table statsdb2.testtable2;

use default;
drop database statsdb1;
drop database statsdb2;


set hive.metastore.try.direct.sql=false;

drop database if exists statsdb1;
create database statsdb1;
drop database if exists statsdb2;
create database statsdb2;

create table statsdb1.testtable1 (col1 int, col2 string, col3 string);
insert into statsdb1.testtable1 select key, value, 'val3' from src limit 10;

create table statsdb1.testpart1 (col1 int, col2 string, col3 string) partitioned by (part string);
insert into statsdb1.testpart1 partition (part = 'part1') select key, value, 'val3' from src limit 10;
insert into statsdb1.testpart1 partition (part = 'part2') select key, value, 'val3' from src limit 20;

use statsdb1;

analyze table testtable1 compute statistics for columns;

describe formatted statsdb1.testtable1 col1;
describe formatted statsdb1.testtable1 col2;
describe formatted statsdb1.testtable1 col3;

alter table testtable1 replace columns (col1 int, col2 string, col4 string);
describe formatted statsdb1.testtable1 col1;
describe formatted statsdb1.testtable1 col2;
describe formatted statsdb1.testtable1 col4;

alter table testtable1 change col1 col1 string;
describe formatted statsdb1.testtable1 col1;
describe formatted statsdb1.testtable1 col2;
describe formatted statsdb1.testtable1 col4;

alter table statsdb1.testtable1 rename to statsdb2.testtable2;


analyze table testpart1 compute statistics for columns;

describe formatted statsdb1.testpart1 col1 partition (part = 'part1');
describe formatted statsdb1.testpart1 col2 partition (part = 'part1');
describe formatted statsdb1.testpart1 col3 partition (part = 'part1');
describe formatted statsdb1.testpart1 col1 partition (part = 'part2');
describe formatted statsdb1.testpart1 col2 partition (part = 'part2');
describe formatted statsdb1.testpart1 col3 partition (part = 'part2');

alter table statsdb1.testpart1 partition (part = 'part2') rename to partition (part = 'part3');
describe formatted statsdb1.testpart1 col1 partition (part = 'part1');
describe formatted statsdb1.testpart1 col2 partition (part = 'part1');
describe formatted statsdb1.testpart1 col3 partition (part = 'part1');
describe formatted statsdb1.testpart1 col1 partition (part = 'part3');
describe formatted statsdb1.testpart1 col2 partition (part = 'part3');
describe formatted statsdb1.testpart1 col3 partition (part = 'part3');

alter table statsdb1.testpart1 replace columns (col1 int, col2 string, col4 string) cascade;
describe formatted statsdb1.testpart1 col1 partition (part = 'part1');
describe formatted statsdb1.testpart1 col2 partition (part = 'part1');
describe formatted statsdb1.testpart1 col4 partition (part = 'part1');

alter table statsdb1.testpart1 change column col1 col1 string;
set hive.exec.dynamic.partition = true;
alter table statsdb1.testpart1 partition (part) change column col1 col1 string;
describe formatted statsdb1.testpart1 col1 partition (part = 'part1');
describe formatted statsdb1.testpart1 col2 partition (part = 'part1');
describe formatted statsdb1.testpart1 col4 partition (part = 'part1');

alter table statsdb1.testpart1 rename to statsdb2.testpart2;
use statsdb2;

alter table statsdb2.testpart2 drop partition (part = 'part1');
drop table statsdb2.testpart2;

drop table statsdb2.testtable2;

use default;
drop database statsdb1;
drop database statsdb2;