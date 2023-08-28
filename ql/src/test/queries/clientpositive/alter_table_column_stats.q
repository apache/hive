--! qt:dataset:src
--! qt:dataset:part
set hive.mapred.mode=nonstrict;

set hive.metastore.try.direct.sql=true;

drop database if exists statsdb1;
create database statsdb1;
drop database if exists statsdb2;
create database statsdb2;

create table statsdb1.testtable0 (col1 int, col2 string, col3 string);
insert into statsdb1.testtable0 select key, value, 'val3' from src limit 10;

create table statsdb1.testpart0 (col1 int, col2 string, col3 string) partitioned by (part string);
insert into statsdb1.testpart0 partition (part = 'part1') select key, value, 'val3' from src limit 10;
insert into statsdb1.testpart0 partition (part = 'part2') select key, value, 'val3' from src limit 20;

use statsdb1;
-- test non-partitioned table
analyze table testtable0 compute statistics for columns;
describe formatted statsdb1.testtable0;
describe formatted statsdb1.testtable0 col1;
describe formatted statsdb1.testtable0 col2;
describe formatted statsdb1.testtable0 col3;

-- rename non-partitioned table should not change its table and columns stats
alter table statsdb1.testtable0 rename to statsdb1.testtable1;
describe formatted statsdb1.testtable1;
describe formatted statsdb1.testtable1 col1;
describe formatted statsdb1.testtable1 col2;
describe formatted statsdb1.testtable1 col3;

-- when replacing columns in a non-partitioned table, the table stats should not change,
-- but the stats of the changed columns are removed
alter table testtable1 replace columns (col1 int, col2 string, col4 string);
describe formatted statsdb1.testtable1;
describe formatted statsdb1.testtable1 col1;
describe formatted statsdb1.testtable1 col2;
describe formatted statsdb1.testtable1 col4;

-- when changing the column type in a non-partitioned table, the table stats should not change,
-- but the stats of the type-changed columns are removed
alter table testtable1 change col1 col1 string;
describe formatted statsdb1.testtable1;
describe formatted statsdb1.testtable1 col1;
describe formatted statsdb1.testtable1 col2;
describe formatted statsdb1.testtable1 col4;

-- rename the db of a non-partitoned table, the table and columns stats should not change
alter table statsdb1.testtable1 rename to statsdb2.testtable2;
describe formatted statsdb2.testtable2;
describe formatted statsdb2.testtable2 col1;
describe formatted statsdb2.testtable2 col2;
describe formatted statsdb2.testtable2 col4;

-- test partitioned table
analyze table testpart0 compute statistics for columns;
describe formatted statsdb1.testpart0;
describe formatted statsdb1.testpart0 partition (part = 'part1');
describe formatted statsdb1.testpart0 partition (part = 'part1') col1;
describe formatted statsdb1.testpart0 partition (part = 'part1') col2;
describe formatted statsdb1.testpart0 partition (part = 'part1') col3;
describe formatted statsdb1.testpart0 partition (part = 'part2');
describe formatted statsdb1.testpart0 partition (part = 'part2') col1;
describe formatted statsdb1.testpart0 partition (part = 'part2') col2;
describe formatted statsdb1.testpart0 partition (part = 'part2') col3;

-- rename a partitioned table should not change its table, partition, and column stats
alter table statsdb1.testpart0 rename to statsdb1.testpart1;
describe formatted statsdb1.testpart1;
describe formatted statsdb1.testpart1 partition (part = 'part1');
describe formatted statsdb1.testpart1 partition (part = 'part1') col1;
describe formatted statsdb1.testpart1 partition (part = 'part1') col2;
describe formatted statsdb1.testpart1 partition (part = 'part1') col3;
describe formatted statsdb1.testpart1 partition (part = 'part2');
describe formatted statsdb1.testpart1 partition (part = 'part2') col1;
describe formatted statsdb1.testpart1 partition (part = 'part2') col2;
describe formatted statsdb1.testpart1 partition (part = 'part2') col3;

-- rename a partition should not change its table, partition, and column stats
alter table statsdb1.testpart1 partition (part = 'part1') rename to partition (part = 'part11');
describe formatted statsdb1.testpart1;
describe formatted statsdb1.testpart1 partition (part = 'part11');
describe formatted statsdb1.testpart1 partition (part = 'part11') col1;
describe formatted statsdb1.testpart1 partition (part = 'part11') col2;
describe formatted statsdb1.testpart1 partition (part = 'part11') col3;
describe formatted statsdb1.testpart1 partition (part = 'part2');
describe formatted statsdb1.testpart1 partition (part = 'part2') col1;
describe formatted statsdb1.testpart1 partition (part = 'part2') col2;
describe formatted statsdb1.testpart1 partition (part = 'part2') col3;

-- when cascade replacing columns in a partitioned table, the table and partition stats should not change,
-- but the stats of the changed columns are removed
alter table statsdb1.testpart1 replace columns (col1 int, col2 string, col4 string) cascade;
describe formatted statsdb1.testpart1;
describe formatted statsdb1.testpart1 partition (part = 'part11');
describe formatted statsdb1.testpart1 partition (part = 'part11') col1;
describe formatted statsdb1.testpart1 partition (part = 'part11') col2;
describe formatted statsdb1.testpart1 partition (part = 'part11') col4;
describe formatted statsdb1.testpart1 partition (part = 'part2');
describe formatted statsdb1.testpart1 partition (part = 'part2') col1;
describe formatted statsdb1.testpart1 partition (part = 'part2') col2;
describe formatted statsdb1.testpart1 partition (part = 'part2') col4;

-- when cascade changing the column type in a partitioned table, the table and partition stats should not change,
-- but the stats of the type-changed columns are removed
alter table statsdb1.testpart1 change column col1 col1 string cascade;
describe formatted statsdb1.testpart1;
describe formatted statsdb1.testpart1 partition (part = 'part11');
describe formatted statsdb1.testpart1 partition (part = 'part11') col1;
describe formatted statsdb1.testpart1 partition (part = 'part11') col2;
describe formatted statsdb1.testpart1 partition (part = 'part11') col4;
describe formatted statsdb1.testpart1 partition (part = 'part2');
describe formatted statsdb1.testpart1 partition (part = 'part2') col1;
describe formatted statsdb1.testpart1 partition (part = 'part2') col2;
describe formatted statsdb1.testpart1 partition (part = 'part2') col4;

-- change database of a partition should not change table, partition and columns stats
alter table statsdb1.testpart1 rename to statsdb2.testpart2;
describe formatted statsdb2.testpart2;
describe formatted statsdb2.testpart2 partition (part = 'part11') col1;
describe formatted statsdb2.testpart2 partition (part = 'part11') col2;
describe formatted statsdb2.testpart2 partition (part = 'part11') col4;
describe formatted statsdb2.testpart2 partition (part = 'part2') col1;
describe formatted statsdb2.testpart2 partition (part = 'part2') col2;
describe formatted statsdb2.testpart2 partition (part = 'part2') col4;

use statsdb2;
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

create table statsdb1.testtable0 (col1 int, col2 string, col3 string);
insert into statsdb1.testtable0 select key, value, 'val3' from src limit 10;

create table statsdb1.testpart0 (col1 int, col2 string, col3 string) partitioned by (part string);
insert into statsdb1.testpart0 partition (part = 'part1') select key, value, 'val3' from src limit 10;
insert into statsdb1.testpart0 partition (part = 'part2') select key, value, 'val3' from src limit 20;

use statsdb1;
-- test non-partitioned table
analyze table testtable0 compute statistics for columns;
describe formatted statsdb1.testtable0;
describe formatted statsdb1.testtable0 col1;
describe formatted statsdb1.testtable0 col2;
describe formatted statsdb1.testtable0 col3;

-- rename non-partitioned table should not change its table and columns stats
alter table statsdb1.testtable0 rename to statsdb1.testtable1;
describe formatted statsdb1.testtable1;
describe formatted statsdb1.testtable1 col1;
describe formatted statsdb1.testtable1 col2;
describe formatted statsdb1.testtable1 col3;

-- when replacing columns in a non-partitioned table, the table stats should not change,
-- but the stats of the changed columns are removed
alter table testtable1 replace columns (col1 int, col2 string, col4 string);
describe formatted statsdb1.testtable1;
describe formatted statsdb1.testtable1 col1;
describe formatted statsdb1.testtable1 col2;
describe formatted statsdb1.testtable1 col4;

-- when changing the column type in a non-partitioned table, the table stats should not change,
-- but the stats of the type-changed columns are removed
alter table testtable1 change col1 col1 string;
describe formatted statsdb1.testtable1;
describe formatted statsdb1.testtable1 col1;
describe formatted statsdb1.testtable1 col2;
describe formatted statsdb1.testtable1 col4;

-- rename the db of a non-partitoned table, the table and columns stats should not change
alter table statsdb1.testtable1 rename to statsdb2.testtable2;
describe formatted statsdb2.testtable2;
describe formatted statsdb2.testtable2 col1;
describe formatted statsdb2.testtable2 col2;
describe formatted statsdb2.testtable2 col4;

-- test partitioned table
analyze table testpart0 compute statistics for columns;
describe formatted statsdb1.testpart0;
describe formatted statsdb1.testpart0 partition (part = 'part1');
describe formatted statsdb1.testpart0 partition (part = 'part1') col1;
describe formatted statsdb1.testpart0 partition (part = 'part1') col2;
describe formatted statsdb1.testpart0 partition (part = 'part1') col3;
describe formatted statsdb1.testpart0 partition (part = 'part2');
describe formatted statsdb1.testpart0 partition (part = 'part2') col1;
describe formatted statsdb1.testpart0 partition (part = 'part2') col2;
describe formatted statsdb1.testpart0 partition (part = 'part2') col3;

-- rename a partitioned table should not change its table, partition, and column stats
alter table statsdb1.testpart0 rename to statsdb1.testpart1;
describe formatted statsdb1.testpart1;
describe formatted statsdb1.testpart1 partition (part = 'part1');
describe formatted statsdb1.testpart1 partition (part = 'part1') col1;
describe formatted statsdb1.testpart1 partition (part = 'part1') col2;
describe formatted statsdb1.testpart1 partition (part = 'part1') col3;
describe formatted statsdb1.testpart1 partition (part = 'part2');
describe formatted statsdb1.testpart1 partition (part = 'part2') col1;
describe formatted statsdb1.testpart1 partition (part = 'part2') col2;
describe formatted statsdb1.testpart1 partition (part = 'part2') col3;

-- rename a partition should not change its table, partition, and column stats
alter table statsdb1.testpart1 partition (part = 'part1') rename to partition (part = 'part11');
describe formatted statsdb1.testpart1;
describe formatted statsdb1.testpart1 partition (part = 'part11');
describe formatted statsdb1.testpart1 partition (part = 'part11') col1;
describe formatted statsdb1.testpart1 partition (part = 'part11') col2;
describe formatted statsdb1.testpart1 partition (part = 'part11') col3;
describe formatted statsdb1.testpart1 partition (part = 'part2');
describe formatted statsdb1.testpart1 partition (part = 'part2') col1;
describe formatted statsdb1.testpart1 partition (part = 'part2') col2;
describe formatted statsdb1.testpart1 partition (part = 'part2') col3;

-- when cascade replacing columns in a partitioned table, the table and partition stats should not change,
-- but the stats of the changed columns are removed
alter table statsdb1.testpart1 replace columns (col1 int, col2 string, col4 string) cascade;
describe formatted statsdb1.testpart1;
describe formatted statsdb1.testpart1 partition (part = 'part11');
describe formatted statsdb1.testpart1 partition (part = 'part11') col1;
describe formatted statsdb1.testpart1 partition (part = 'part11') col2;
describe formatted statsdb1.testpart1 partition (part = 'part11') col4;
describe formatted statsdb1.testpart1 partition (part = 'part2');
describe formatted statsdb1.testpart1 partition (part = 'part2') col1;
describe formatted statsdb1.testpart1 partition (part = 'part2') col2;
describe formatted statsdb1.testpart1 partition (part = 'part2') col4;

-- when cascade changing the column type in a partitioned table, the table and partition stats should not change,
-- but the stats of the type-changed columns are removed
alter table statsdb1.testpart1 change column col1 col1 string cascade;
describe formatted statsdb1.testpart1;
describe formatted statsdb1.testpart1 partition (part = 'part11');
describe formatted statsdb1.testpart1 partition (part = 'part11') col1;
describe formatted statsdb1.testpart1 partition (part = 'part11') col2;
describe formatted statsdb1.testpart1 partition (part = 'part11') col4;
describe formatted statsdb1.testpart1 partition (part = 'part2');
describe formatted statsdb1.testpart1 partition (part = 'part2') col1;
describe formatted statsdb1.testpart1 partition (part = 'part2') col2;
describe formatted statsdb1.testpart1 partition (part = 'part2') col4;

-- change database of a partition should not change table, partition and columns stats
alter table statsdb1.testpart1 rename to statsdb2.testpart2;
describe formatted statsdb2.testpart2;
describe formatted statsdb2.testpart2 partition (part = 'part11') col1;
describe formatted statsdb2.testpart2 partition (part = 'part11') col2;
describe formatted statsdb2.testpart2 partition (part = 'part11') col4;
describe formatted statsdb2.testpart2 partition (part = 'part2') col1;
describe formatted statsdb2.testpart2 partition (part = 'part2') col2;
describe formatted statsdb2.testpart2 partition (part = 'part2') col4;

use statsdb2;
drop table statsdb2.testpart2;
drop table statsdb2.testtable2;

use default;
drop database statsdb1;
drop database statsdb2;

-- Test for external tables with hive.metastore.try.direct.sql.ddl as false
set hive.metastore.try.direct.sql.ddl=false;

drop database if exists statsdb1;
create database statsdb1;

create external table statsdb1.testtable0 (col1 int, col2 string, col3 string) row format delimited fields terminated by ',' stored as textfile tblproperties ('external.table.purge'='true');
insert into statsdb1.testtable0 select key, value, 'val3' from src limit 10;

create external table statsdb1.testpart0 (col1 int, col2 string, col3 string) partitioned by (part string) row format delimited fields terminated by ',' stored as textfile tblproperties ('external.table.purge'='true');
insert into statsdb1.testpart0 partition (part = 'part1') select key, value, 'val3' from src limit 10;

use statsdb1;
-- test non-partitioned table
analyze table testtable0 compute statistics for columns;
describe formatted statsdb1.testtable0;
describe formatted statsdb1.testtable0 col1;

-- rename non-partitioned table should not change its table and columns stats
alter table statsdb1.testtable0 rename to statsdb1.testtable1;
describe formatted statsdb1.testtable1;
describe formatted statsdb1.testtable1 col1;

-- test partitioned table
analyze table testpart0 compute statistics for columns;
describe formatted statsdb1.testpart0;
describe formatted statsdb1.testpart0 partition (part = 'part1');
describe formatted statsdb1.testpart0 partition (part = 'part1') col1;

-- rename a partitioned table should not change its table, partition, and column stats
alter table statsdb1.testpart0 rename to statsdb1.testpart1;
describe formatted statsdb1.testpart1;
describe formatted statsdb1.testpart1 partition (part = 'part1');
describe formatted statsdb1.testpart1 partition (part = 'part1') col1;

drop table statsdb1.testpart1;
drop table statsdb1.testtable1;

use default;
drop database statsdb1;