
create database ttp;

-- Create non-temp tables
create table ttp.tab1 (a1 string, a2 string);
insert overwrite table ttp.tab1 select * from src where key = 5 limit 5;
describe ttp.tab1;
select * from ttp.tab1;
create table ttp.tab2 (b1 string, b2 string);
insert overwrite table ttp.tab2 select * from src where key = 2 limit 5;
describe ttp.tab2;
select * from ttp.tab2;

-- Now create temp table with same name
create temporary table ttp.tab1 (c1 int, c2 string);
insert overwrite table ttp.tab1 select * from src where key = 0 limit 5;

-- describe/select should now use temp table
describe ttp.tab1;
select * from ttp.tab1;

-- rename the temp table, and now we can see our non-temp table again
use ttp;
alter table tab1 rename to tab2;
use default;
describe ttp.tab1;
select * from ttp.tab1;

-- now the non-temp tab2 should be hidden
describe ttp.tab2;
select * from ttp.tab2;

-- drop the temp table, and now we should be able to see the non-temp tab2 again
drop table ttp.tab2;
describe ttp.tab2;
select * from ttp.tab2;

drop database ttp cascade;
