--! qt:dataset:srcpart
set hive.archive.enabled = true;
-- Tests trying to insert into archived partition.

CREATE TABLE tstsrcpart LIKE srcpart;

INSERT OVERWRITE TABLE tstsrcpart PARTITION (ds='2008-04-08', hr='12')
SELECT key, value FROM srcpart WHERE ds='2008-04-08' AND hr='12';

ALTER TABLE tstsrcpart ARCHIVE PARTITION (ds='2008-04-08');

INSERT OVERWRITE TABLE tstsrcpart PARTITION (ds='2008-04-08', hr='12')
SELECT key, value FROM srcpart WHERE ds='2008-04-08' AND hr='12';
