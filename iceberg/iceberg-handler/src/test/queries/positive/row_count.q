-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
-- Mask random snapshot id
--! qt:replace:/(\s+current-snapshot-id\s+)\S+(\s*)/$1#SnapshotId#/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/(\s+current-snapshot-timestamp-ms\s+)\S+(\s*)/$1#Masked#/
-- Mask totalSize
--! qt:replace:/(\s+totalSize\s+)\S+(\s*)/$1#Masked#/
-- Mask added file size
--! qt:replace:/(\S\"added-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask total file size
--! qt:replace:/(\S\"total-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask width
--! qt:replace:/(width=15)\d+/$1###/

drop table if exists llap_orders;

CREATE EXTERNAL TABLE llap_orders (orderid INT, quantity INT, itemid INT, tradets TIMESTAMP) PARTITIONED BY (p1 STRING, p2 STRING) STORED BY ICEBERG STORED AS ORC tblproperties('format-version'='2');


INSERT INTO llap_orders VALUES
(0, 48, 5, timestamp('2000-06-04 19:55:46.129'), 'EU', 'DE'),
(1, 12, 6, timestamp('2007-06-24 19:23:22.829'), 'US', 'TX'),
(2, 76, 4, timestamp('2018-02-19 23:43:51.995'), 'EU', 'DE'),
(3, 91, 5, timestamp('2000-07-15 09:09:11.587'), 'US', 'NJ'),
(4, 18, 6, timestamp('2007-12-02 22:30:39.302'), 'EU', 'ES'),
(5, 71, 5, timestamp('2010-02-08 20:31:23.430'), 'EU', 'DE'),
(6, 78, 3, timestamp('2016-02-22 20:37:37.025'), 'EU', 'FR'),
(7, 88, 0, timestamp('2020-03-26 18:47:40.611'), 'EU', 'FR'),
(8, 87, 4, timestamp('2003-02-20 00:48:09.139'), 'EU', 'ES'),
(9, 60, 6, timestamp('2012-08-28 01:35:54.283'), 'EU', 'IT'),
(10, 24, 5, timestamp('2015-03-28 18:57:50.069'), 'US', 'NY'),
(11, 42, 2, timestamp('2012-06-27 01:13:32.350'), 'EU', 'UK'),
(12, 37, 4, timestamp('2020-08-09 01:18:50.153'), 'US', 'NY'),
(13, 52, 1, timestamp('2019-09-04 01:46:19.558'), 'EU', 'UK'),
(14, 96, 3, timestamp('2019-03-05 22:00:03.020'), 'US', 'NJ'),
(15, 18, 3, timestamp('2001-09-11 00:14:12.687'), 'EU', 'FR'),
(16, 46, 0, timestamp('2013-08-31 02:16:17.878'), 'EU', 'UK'),
(17, 26, 5, timestamp('2001-02-01 20:05:32.317'), 'EU', 'FR'),
(18, 68, 5, timestamp('2009-12-29 08:44:08.048'), 'EU', 'ES'),
(19, 54, 6, timestamp('2015-08-15 01:59:22.177'), 'EU', 'HU'),
(20, 10, 0, timestamp('2018-05-06 12:56:12.789'), 'US', 'CA');

--check row count
select count(*) from  llap_orders;
describe formatted llap_orders;

--delete rows
delete from llap_orders where itemid = 6;
delete from llap_orders where itemid = 5;

--check for updated row count
select count(*) from  llap_orders;
describe formatted llap_orders;

explain select count(*) from  llap_orders;
explain insert into  llap_orders select * from llap_orders limit 100000;
