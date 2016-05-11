set hive.mapred.mode=nonstrict;
set hive.metastore.try.direct.sql=true;

drop database if exists statsdb1;
create database statsdb1;
drop database if exists statsdb2;
create database statsdb2;

create table statsdb1.testtable1 (col1 int, col2 string, col3 string);
insert into statsdb1.testtable1 select key, value, 'val3' from src limit 10;

use statsdb1;

analyze table testtable1 compute statistics for columns;

describe formatted statsdb1.testtable1 col1;
describe formatted statsdb1.testtable1 col2;
describe formatted statsdb1.testtable1 col3;

alter table statsdb1.testtable1 rename to statsdb2.testtable2;
describe formatted statsdb2.testtable2 col1;
describe formatted statsdb2.testtable2 col2;
describe formatted statsdb2.testtable2 col3;

use default;
drop database statsdb1 cascade;
drop database statsdb2 cascade;


set hive.metastore.try.direct.sql=false;

drop database if exists statsdb1;
create database statsdb1;
drop database if exists statsdb2;
create database statsdb2;

create table statsdb1.testtable1 (col1 int, col2 string, col3 string);
insert into statsdb1.testtable1 select key, value, 'val3' from src limit 10;

use statsdb1;

analyze table testtable1 compute statistics for columns;

describe formatted statsdb1.testtable1 col1;
describe formatted statsdb1.testtable1 col2;
describe formatted statsdb1.testtable1 col3;

alter table statsdb1.testtable1 rename to statsdb2.testtable2;
describe formatted statsdb2.testtable2 col1;
describe formatted statsdb2.testtable2 col2;
describe formatted statsdb2.testtable2 col3;

use default;
drop database statsdb1 cascade;
drop database statsdb2 cascade;
