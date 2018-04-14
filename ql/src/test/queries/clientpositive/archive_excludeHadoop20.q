--! qt:dataset:srcpart
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.archive.enabled = true;
set hive.exec.submitviachild=false;
set hive.exec.submit.local.task.via.child=false;

drop table tstsrc_n2;
drop table tstsrcpart_n2;

create table tstsrc_n2 like src;
insert overwrite table tstsrc_n2 select key, value from src;

create table tstsrcpart_n2 (key string, value string) partitioned by (ds string, hr string) clustered by (key) into 10 buckets;

insert overwrite table tstsrcpart_n2 partition (ds='2008-04-08', hr='11')
select key, value from srcpart where ds='2008-04-08' and hr='11';

insert overwrite table tstsrcpart_n2 partition (ds='2008-04-08', hr='12')
select key, value from srcpart where ds='2008-04-08' and hr='12';

insert overwrite table tstsrcpart_n2 partition (ds='2008-04-09', hr='11')
select key, value from srcpart where ds='2008-04-09' and hr='11';

insert overwrite table tstsrcpart_n2 partition (ds='2008-04-09', hr='12')
select key, value from srcpart where ds='2008-04-09' and hr='12';

SELECT SUM(hash(col)) FROM (SELECT transform(*) using 'tr "\t" "_"' AS col
FROM (SELECT * FROM tstsrcpart_n2 WHERE ds='2008-04-08') subq1) subq2;

ALTER TABLE tstsrcpart_n2 ARCHIVE PARTITION (ds='2008-04-08', hr='12');

SELECT SUM(hash(col)) FROM (SELECT transform(*) using 'tr "\t" "_"' AS col
FROM (SELECT * FROM tstsrcpart_n2 WHERE ds='2008-04-08') subq1) subq2;

SELECT key, count(1) FROM tstsrcpart_n2 WHERE ds='2008-04-08' AND hr='12' AND key='0' GROUP BY key;

SELECT * FROM tstsrcpart_n2 a JOIN tstsrc_n2 b ON a.key=b.key
WHERE a.ds='2008-04-08' AND a.hr='12' AND a.key='0';

ALTER TABLE tstsrcpart_n2 UNARCHIVE PARTITION (ds='2008-04-08', hr='12');

SELECT SUM(hash(col)) FROM (SELECT transform(*) using 'tr "\t" "_"' AS col
FROM (SELECT * FROM tstsrcpart_n2 WHERE ds='2008-04-08') subq1) subq2;

CREATE TABLE harbucket(key INT)
PARTITIONED by (ds STRING)
CLUSTERED BY (key) INTO 10 BUCKETS;

INSERT OVERWRITE TABLE harbucket PARTITION(ds='1') SELECT CAST(key AS INT) AS a FROM tstsrc_n2 WHERE key > 50;

SELECT key FROM harbucket TABLESAMPLE(BUCKET 1 OUT OF 10) SORT BY key;
ALTER TABLE tstsrcpart_n2 ARCHIVE PARTITION (ds='2008-04-08', hr='12');
SELECT key FROM harbucket TABLESAMPLE(BUCKET 1 OUT OF 10) SORT BY key;
ALTER TABLE tstsrcpart_n2 UNARCHIVE PARTITION (ds='2008-04-08', hr='12');
SELECT key FROM harbucket TABLESAMPLE(BUCKET 1 OUT OF 10) SORT BY key;


CREATE TABLE old_name(key INT)
PARTITIONED by (ds STRING);

INSERT OVERWRITE TABLE old_name PARTITION(ds='1') SELECT CAST(key AS INT) AS a FROM tstsrc_n2 WHERE key > 50;
ALTER TABLE old_name ARCHIVE PARTITION (ds='1');
SELECT SUM(hash(col)) FROM (SELECT transform(*) using 'tr "\t" "_"' AS col
FROM (SELECT * FROM old_name WHERE ds='1') subq1) subq2;
ALTER TABLE old_name RENAME TO new_name;
SELECT SUM(hash(col)) FROM (SELECT transform(*) using 'tr "\t" "_"' AS col
FROM (SELECT * FROM new_name WHERE ds='1') subq1) subq2;

drop table tstsrc_n2;
drop table tstsrcpart_n2;
