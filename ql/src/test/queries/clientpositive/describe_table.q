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

CREATE DATABASE IF NOT EXISTS name1;
CREATE DATABASE IF NOT EXISTS name2;
use name1;
CREATE TABLE IF NOT EXISTS name1 (name1 int, name2 string) PARTITIONED BY (name3 int);
ALTER TABLE name1 ADD PARTITION (name3=1);
CREATE TABLE IF NOT EXISTS name2 (name3 int, name4 string);
use name2;
CREATE TABLE IF NOT EXISTS table1 (col1 int, col2 string);

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

DESCRIBE name2.table1;
DESCRIBE name2.table1 col1;
DESCRIBE name2.table1 col2;
use name2;
DESCRIBE table1;
DESCRIBE table1 col1;
DESCRIBE table1 col2;

DESCRIBE name2.table1;
DESCRIBE name2.table1 col1;
DESCRIBE name2.table1 col2;

DROP TABLE IF EXISTS table1;
use name1;
DROP TABLE IF EXISTS name1;
DROP TABLE IF EXISTS name2;
use name2;
DROP TABLE IF EXISTS table1;
DROP DATABASE IF EXISTS name1;
DROP DATABASE IF EXISTS name2;
