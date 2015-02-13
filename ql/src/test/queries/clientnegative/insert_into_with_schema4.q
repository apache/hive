-- set of tests HIVE-9481
drop database if exists x314n cascade;
create database x314n;
use x314n;

CREATE TABLE pageviews (userid VARCHAR(64), link STRING, source STRING) PARTITIONED BY (datestamp STRING, i int) CLUSTERED BY (userid) INTO 256 BUCKETS STORED AS ORC;
--datestamp is a static partition thus should not be supplied by producer side
INSERT INTO TABLE pageviews PARTITION (datestamp='2014-09-23',i)(userid,i,datestamp,link) VALUES ('jsmith', 7, '2014-07-12', '7mail.com');

drop database if exists x314n cascade;
