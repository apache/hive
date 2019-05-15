--! qt:dataset:srcpart
set hive.archive.enabled = true;
-- Tests trying to archive a partition twice.

CREATE TABLE srcpart_archived (key string, value string) partitioned by (ds string, hr int, min int);

INSERT OVERWRITE TABLE srcpart_archived PARTITION (ds='2008-04-08', hr='12', min='00')
SELECT key, value FROM srcpart WHERE ds='2008-04-08' AND hr='12';

ALTER TABLE srcpart_archived ARCHIVE PARTITION (ds='2008-04-08', min='00');