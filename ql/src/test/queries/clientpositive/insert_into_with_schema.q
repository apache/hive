set hive.mapred.mode=nonstrict;
-- set of tests HIVE-9481

drop database if exists x314 cascade;
create database x314;
use x314;
create table source_n0(s1 int, s2 int);
create table target1(x int, y int, z int);
create table target2(x int, y int, z int);
create table target3(x int, y int, z int);

insert into source_n0(s2,s1) values(2,1);
-- expect source_n0 to contain 1 row (1,2)
select * from source_n0;
insert into target1(z,x) select * from source_n0;
-- expect target1 to contain 1 row (2,NULL,1)
select * from target1;

-- note that schema spec for target1 and target2 are different
from source_n0 insert into target1(x,y) select * insert into target2(x,z) select s2,s1;
--expect target1 to have 2rows (2,NULL,1), (1,2,NULL)
select * from target1 order by x,y,z;
-- expect target2 to have 1 row: (2,NULL,1)
select * from target2;


from source_n0 insert into target1(x,y,z) select null as x, * insert into target2(x,y,z) select null as x, source_n0.*;
-- expect target1 to have 3 rows: (2,NULL,1), (1,2,NULL), (NULL, 1,2)
select * from target1 order by x,y,z;
-- expect target2 to have 2 rows: (2,NULL,1), (NULL, 1,2)
select * from target2 order by x,y,z;

create table source2(s1 int, s2 int);
insert into target3 (x,z) select source_n0.s1,source2.s2 from source_n0 left outer join source2 on source_n0.s1=source2.s2;
--expect target3 to have 1 row (1,NULL,NULL)
select * from target3;


-- partitioned tables
CREATE TABLE pageviews (userid VARCHAR(64), link STRING, source_n0 STRING) PARTITIONED BY (datestamp STRING, i int) CLUSTERED BY (userid) INTO 4 BUCKETS STORED AS ORC;
INSERT INTO TABLE pageviews PARTITION (datestamp = '2014-09-23', i = 1)(userid,link) VALUES ('jsmith', 'mail.com');
-- expect 1 row: ('jsmith', 'mail.com', NULL) in partition '2014-09-23'/'1'
select * from pageviews;


-- dynamic partitioning 



INSERT INTO TABLE pageviews PARTITION (datestamp='2014-09-23',i)(userid,i,link) VALUES ('jsmith', 7, '7mail.com');

set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE pageviews PARTITION (datestamp,i)(userid,i,link,datestamp) VALUES ('jsmith', 17, '17mail.com', '2014-09-23');
INSERT INTO TABLE pageviews PARTITION (datestamp,i)(userid,i,link,datestamp) VALUES ('jsmith', 19, '19mail.com', '2014-09-24');
-- here the 'datestamp' partition column is not provided and will be NULL-filled
INSERT INTO TABLE pageviews PARTITION (datestamp,i)(userid,i,link) VALUES ('jsmith', 23, '23mail.com');
-- expect 5 rows:
-- expect ('jsmith', 'mail.com', NULL) in partition '2014-09-23'/'1'
-- expect ('jsmith', '7mail.com', NULL) in partition '2014-09-23'/'7'
-- expect ('jsmith', '17mail.com', NULL) in partition '2014-09-23'/'17'
-- expect ('jsmith', '19mail.com', NULL) in partition '2014-09-24'/'19'
-- expect ('jsmith', '23mail.com', NULL) in partition '__HIVE_DEFAULT_PARTITION__'/'23'
select * from pageviews order by link;


drop database if exists x314 cascade;
