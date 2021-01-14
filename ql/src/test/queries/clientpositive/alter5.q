--
-- Added to validate the fix for HIVE-2117 - explicit partition location
--

create table alter5_src ( col1 string ) stored as textfile ;
load data local inpath '../../data/files/test.dat' overwrite into table alter5_src ;

create table alter5_n1 ( col1 string ) partitioned by (dt string);

--
-- Here's the interesting bit for HIVE-2117 - partition subdir should be
-- named "parta".
--
alter table alter5_n1 add partition (dt='a') location 'parta';

describe extended alter5_n1 partition (dt='a');

insert overwrite table alter5_n1 partition (dt='a') select col1 from alter5_src ;
select * from alter5_n1 where dt='a';

describe extended alter5_n1 partition (dt='a');

-- Cleanup
DROP TABLE alter5_src;
DROP TABLE alter5_n1;
SHOW TABLES LIKE "alter%";

-- With non-default Database

CREATE DATABASE alter5_db;
USE alter5_db;
SHOW TABLES;

create table alter5_src ( col1 string ) stored as textfile ;
load data local inpath '../../data/files/test.dat' overwrite into table alter5_src ;

create table alter5_n1 ( col1 string ) partitioned by (dt string);
alter table alter5_n1 add partition (dt='a') location 'parta';

describe extended alter5_n1 partition (dt='a');

insert overwrite table alter5_n1 partition (dt='a') select col1 from alter5_src ;
select * from alter5_n1 where dt='a';

describe extended alter5_n1 partition (dt='a');
