--! qt:dataset:part

CREATE DATABASE db1;
CREATE TABLE db1.t1(key1 INT, value1 STRING) PARTITIONED BY (ds STRING, part STRING);

use db1;

ALTER TABLE t1 ADD PARTITION (ds='3', part='3');
ALTER TABLE t1 ADD PARTITION (ds='4', part='4');
ALTER TABLE t1 ADD PARTITION (ds='4', part='5');

-- describe table
DESCRIBE t1;
DESCRIBE EXTENDED t1;
DESCRIBE FORMATTED t1;

-- describe database.table
DESCRIBE db1.t1;
DESCRIBE EXTENDED db1.t1;
DESCRIBE FORMATTED db1.t1;

-- describe table column
DESCRIBE t1 key1;
DESCRIBE EXTENDED t1 key1;
DESCRIBE FORMATTED t1 key1;

-- describe database.tabe column
DESCRIBE db1.t1 key1;
DESCRIBE EXTENDED db1.t1 key1;
DESCRIBE FORMATTED db1.t1 key1;

-- describe table column
DESCRIBE t1 key1;
DESCRIBE EXTENDED t1 key1;
DESCRIBE FORMATTED t1 key1;

-- describe table partition column
DESCRIBE t1 ds;
DESCRIBE EXTENDED t1 ds;
DESCRIBE FORMATTED t1 ds;

-- describe database.table partition column
DESCRIBE db1.t1 ds;
DESCRIBE EXTENDED db1.t1 ds;
DESCRIBE FORMATTED db1.t1 ds;

-- describe table case-insensitive partition column
DESCRIBE t1 PART;

-- describe table partition
DESCRIBE t1 PARTITION(ds='4', part='5');
DESCRIBE EXTENDED t1 PARTITION(ds='4', part='5');
DESCRIBE FORMATTED t1 PARTITION(ds='4', part='5');

-- describe database.table partition
DESCRIBE db1.t1 PARTITION(ds='4', part='5');
DESCRIBE EXTENDED db1.t1 PARTITION(ds='4', part='5');
DESCRIBE FORMATTED db1.t1 PARTITION(ds='4', part='5');

-- describe table partition column with partition spec
DESCRIBE t1 PARTITION(ds='3', part='3') ds;
DESCRIBE EXTENDED t1 PARTITION(ds='4', part='5') part;
DESCRIBE FORMATTED t1 PARTITION(ds='3', part='3') ds;
