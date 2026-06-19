set hive.mapred.mode=nonstrict;
CREATE TABLE tstparttbl_n0(KEY STRING, VALUE STRING) PARTITIONED BY(ds string) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE tstparttbl_n0 PARTITION (ds='2008-04-09');
LOAD DATA LOCAL INPATH '../../data/files/nullfile.txt' INTO TABLE tstparttbl_n0 PARTITION (ds='2008-04-08');
explain
select count(1) from tstparttbl_n0;
select count(1) from tstparttbl_n0;

CREATE TABLE tstparttbl2_n0(KEY STRING, VALUE STRING) PARTITIONED BY(ds string) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/nullfile.txt' INTO TABLE tstparttbl2_n0 PARTITION (ds='2008-04-09');
LOAD DATA LOCAL INPATH '../../data/files/nullfile.txt' INTO TABLE tstparttbl2_n0 PARTITION (ds='2008-04-08');
explain
select count(1) from tstparttbl2_n0;
select count(1) from tstparttbl2_n0;
DROP TABLE tstparttbl_n0;
CREATE TABLE tstparttbl_n0(KEY STRING, VALUE STRING) PARTITIONED BY(ds string) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE tstparttbl_n0 PARTITION (ds='2008-04-09');
LOAD DATA LOCAL INPATH '../../data/files/nullfile.txt' INTO TABLE tstparttbl_n0 PARTITION (ds='2008-04-08');
explain
select count(1) from tstparttbl_n0;
select count(1) from tstparttbl_n0;

DROP TABLE tstparttbl2_n0;
CREATE TABLE tstparttbl2_n0(KEY STRING, VALUE STRING) PARTITIONED BY(ds string) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/nullfile.txt' INTO TABLE tstparttbl2_n0 PARTITION (ds='2008-04-09');
LOAD DATA LOCAL INPATH '../../data/files/nullfile.txt' INTO TABLE tstparttbl2_n0 PARTITION (ds='2008-04-08');
explain
select count(1) from tstparttbl2_n0;
select count(1) from tstparttbl2_n0;
