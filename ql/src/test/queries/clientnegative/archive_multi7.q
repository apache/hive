--! qt:dataset:srcpart
set hive.archive.enabled = true;
-- Tests trying to archive a partition group with custom locations.

CREATE TABLE tstsrcpart LIKE srcpart;

INSERT OVERWRITE TABLE tstsrcpart PARTITION (ds='2008-04-08', hr='11')
SELECT key, value FROM srcpart WHERE ds='2008-04-08' AND hr='11';
ALTER TABLE tstsrcpart ADD PARTITION (ds='2008-04-08', hr='12')
LOCATION "${system:test.tmp.dir}/tstsrc";

ALTER TABLE tstsrcpart ARCHIVE PARTITION (ds='2008-04-08');
