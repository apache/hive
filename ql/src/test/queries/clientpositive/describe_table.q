--! qt:dataset:srcpart
describe srcpart;
describe srcpart key;
describe srcpart PARTITION(ds='2008-04-08', hr='12');

describe `srcpart`;
describe `srcpart` `key`;
describe `srcpart` PARTITION(ds='2008-04-08', hr='12');

describe extended srcpart;
describe extended srcpart key;
describe extended srcpart PARTITION(ds='2008-04-08', hr='12');

describe extended `srcpart`;
describe extended `srcpart` `key`;
describe extended `srcpart` PARTITION(ds='2008-04-08', hr='12');

describe formatted srcpart;
describe formatted srcpart key;
describe formatted srcpart PARTITION(ds='2008-04-08', hr='12');

describe formatted `srcpart`;
describe formatted `srcpart` `key`;
describe formatted `srcpart` PARTITION(ds='2008-04-08', hr='12');

describe formatted `srcpart` `ds`;
describe formatted `srcpart` `hr`;

create table srcpart_serdeprops like srcpart;
alter table srcpart_serdeprops set serdeproperties('xyz'='0');
alter table srcpart_serdeprops set serdeproperties('pqrs'='1');
alter table srcpart_serdeprops set serdeproperties('abcd'='2');
alter table srcpart_serdeprops set serdeproperties('A1234'='3');
describe formatted srcpart_serdeprops;
drop table srcpart_serdeprops;

CREATE TABLE IF NOT EXISTS desc_parttable_stats (somenumber int) PARTITIONED BY (yr int);
INSERT INTO desc_parttable_stats values(0,1),(0,2),(0,3);
set hive.describe.partitionedtable.ignore.stats=true;
describe formatted desc_parttable_stats;
set hive.describe.partitionedtable.ignore.stats=false;
describe formatted desc_parttable_stats;
DROP TABLE IF EXISTS desc_parttable_stats;

CREATE DATABASE IF NOT EXISTS name1;
CREATE DATABASE IF NOT EXISTS name2;
use name1;
CREATE TABLE IF NOT EXISTS name1 (name1 int, name2 string) PARTITIONED BY (name3 int);
ALTER TABLE name1 ADD PARTITION (name3=1);
CREATE TABLE IF NOT EXISTS name2 (name3 int, name4 string);
use name2;
CREATE TABLE IF NOT EXISTS table1_n18 (col1 int, col2 string);

use default;
DESCRIBE name1.name1;
DESCRIBE name1.name1 name2;
DESCRIBE name1.name1 PARTITION (name3=1);
DESCRIBE name1.name2;
DESCRIBE name1.name2 name3;
DESCRIBE name1.name2 name4;

use name1;
DESCRIBE name1;
DESCRIBE name1 name2;
DESCRIBE name1 PARTITION (name3=1);
DESCRIBE name1.name1;
DESCRIBE name1.name1 name2;
DESCRIBE name1.name1 PARTITION (name3=1);
DESCRIBE name2;
DESCRIBE name2 name3;
DESCRIBE name2 name4;
DESCRIBE name1.name2;
DESCRIBE name1.name2 name3;
DESCRIBE name1.name2 name4;

DESCRIBE name2.table1_n18;
DESCRIBE name2.table1_n18 col1;
DESCRIBE name2.table1_n18 col2;
use name2;
DESCRIBE table1_n18;
DESCRIBE table1_n18 col1;
DESCRIBE table1_n18 col2;

DESCRIBE name2.table1_n18;
DESCRIBE name2.table1_n18 col1;
DESCRIBE name2.table1_n18 col2;

DROP TABLE IF EXISTS table1_n18;
use name1;
DROP TABLE IF EXISTS name1;
DROP TABLE IF EXISTS name2;
use name2;
DROP TABLE IF EXISTS table1_n18;
DROP DATABASE IF EXISTS name1;
DROP DATABASE IF EXISTS name2;
