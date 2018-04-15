--! qt:dataset:src
--! qt:dataset:part
set hive.mapred.mode=nonstrict;
-- Test type date, int, and string in partition column
drop table if exists partcolstats;

create table partcolstats (key int, value string) partitioned by (ds date, hr int, part string);
insert into partcolstats partition (ds=date '2015-04-02', hr=2, part='partA') select key, value from src limit 20;
insert into partcolstats partition (ds=date '2015-04-02', hr=2, part='partB') select key, value from src limit 20;
insert into partcolstats partition (ds=date '2015-04-02', hr=3, part='partA') select key, value from src limit 30;
insert into partcolstats partition (ds=date '2015-04-03', hr=3, part='partA') select key, value from src limit 40;
insert into partcolstats partition (ds=date '2015-04-03', hr=3, part='partB') select key, value from src limit 60;

analyze table partcolstats partition (ds=date '2015-04-02', hr=2, part='partA') compute statistics for columns;
describe formatted partcolstats partition (ds=date '2015-04-02', hr=2, part='partA') key;
describe formatted partcolstats partition (ds=date '2015-04-02', hr=2, part='partA') value;

describe formatted partcolstats partition (ds=date '2015-04-02', hr=2, part='partB') key;
describe formatted partcolstats partition (ds=date '2015-04-02', hr=2, part='partB') value;

analyze table partcolstats partition (ds=date '2015-04-02', hr=2, part) compute statistics for columns;
describe formatted partcolstats partition (ds=date '2015-04-02', hr=2, part='partB') key;
describe formatted partcolstats partition (ds=date '2015-04-02', hr=2, part='partB') value;

describe formatted partcolstats partition (ds=date '2015-04-02', hr=3, part='partA') key;
describe formatted partcolstats partition (ds=date '2015-04-02', hr=3, part='partA') value;

analyze table partcolstats partition (ds=date '2015-04-02', hr, part) compute statistics for columns;
describe formatted partcolstats partition (ds=date '2015-04-02', hr=3, part='partA') key;
describe formatted partcolstats partition (ds=date '2015-04-02', hr=3, part='partA') value;

describe formatted partcolstats partition (ds=date '2015-04-03', hr=3, part='partA') key;
describe formatted partcolstats partition (ds=date '2015-04-03', hr=3, part='partA') value;
describe formatted partcolstats partition (ds=date '2015-04-03', hr=3, part='partB') key;
describe formatted partcolstats partition (ds=date '2015-04-03', hr=3, part='partB') value;

analyze table partcolstats partition (ds, hr, part) compute statistics for columns;
describe formatted partcolstats partition (ds=date '2015-04-03', hr=3, part='partA') key;
describe formatted partcolstats partition (ds=date '2015-04-03', hr=3, part='partA') value;
describe formatted partcolstats partition (ds=date '2015-04-03', hr=3, part='partB') key;
describe formatted partcolstats partition (ds=date '2015-04-03', hr=3, part='partB') value;

drop table partcolstats;

-- Test type tinyint, smallint, and bigint in partition column
drop table if exists partcolstatsnum;
create table partcolstatsnum (key int, value string) partitioned by (tint tinyint, sint smallint, bint bigint);
insert into partcolstatsnum partition (tint=100, sint=1000, bint=1000000) select key, value from src limit 30;

analyze table partcolstatsnum partition (tint=100, sint=1000, bint=1000000) compute statistics for columns;
describe formatted partcolstatsnum partition (tint=100, sint=1000, bint=1000000) value;

drop table partcolstatsnum;

-- Test type decimal in partition column
drop table if exists partcolstatsdec;
create table partcolstatsdec (key int, value string) partitioned by (decpart decimal(8,4));
insert into partcolstatsdec partition (decpart='1000.0001') select key, value from src limit 30;

analyze table partcolstatsdec partition (decpart='1000.0001') compute statistics for columns;
describe formatted partcolstatsdec partition (decpart='1000.0001') value;

drop table partcolstatsdec;

-- Test type varchar and char in partition column
drop table if exists partcolstatschar;
create table partcolstatschar (key int, value string) partitioned by (varpart varchar(5), charpart char(3));
insert into partcolstatschar partition (varpart='part1', charpart='aaa') select key, value from src limit 30;

analyze table partcolstatschar partition (varpart='part1', charpart='aaa') compute statistics for columns;
describe formatted partcolstatschar partition (varpart='part1', charpart='aaa') value;

drop table partcolstatschar;

