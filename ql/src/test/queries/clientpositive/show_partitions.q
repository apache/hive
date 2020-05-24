--! qt:dataset:srcpart
SHOW PARTITIONS srcpart;
SHOW PARTITIONS default.srcpart;
SHOW PARTITIONS srcpart PARTITION(hr='11');
SHOW PARTITIONS srcpart PARTITION(ds='2008-04-08');
SHOW PARTITIONS srcpart PARTITION(ds='2008-04-08', hr='12');


SHOW PARTITIONS default.srcpart;
SHOW PARTITIONS default.srcpart PARTITION(hr='11');
SHOW PARTITIONS default.srcpart PARTITION(ds='2008-04-08');
SHOW PARTITIONS default.srcpart PARTITION(ds='2008-04-08', hr='12');

CREATE DATABASE db1;
USE db1;

CREATE TABLE srcpart (key1 INT, value1 STRING) PARTITIONED BY (ds STRING, hr STRING);
ALTER TABLE srcpart ADD PARTITION (ds='3', hr='3');
ALTER TABLE srcpart ADD PARTITION (ds='4', hr='4');
ALTER TABLE srcpart ADD PARTITION (ds='4', hr='5');

-- from db1 to default db
EXPLAIN SHOW PARTITIONS default.srcpart PARTITION(hr='11');
SHOW PARTITIONS default.srcpart PARTITION(hr='11');
EXPLAIN SHOW PARTITIONS default.srcpart PARTITION(ds='2008-04-08', hr='12');
SHOW PARTITIONS default.srcpart PARTITION(ds='2008-04-08', hr='12');

-- from db1 to db1
SHOW PARTITIONS srcpart PARTITION(ds='4');
SHOW PARTITIONS srcpart PARTITION(ds='3', hr='3');

use default;
-- from default to db1
SHOW PARTITIONS db1.srcpart PARTITION(ds='4');
SHOW PARTITIONS db1.srcpart PARTITION(ds='3', hr='3');
