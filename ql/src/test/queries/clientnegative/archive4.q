--! qt:dataset:srcpart
set hive.archive.enabled = true;
-- Tests archiving multiple partitions

ALTER TABLE srcpart ARCHIVE PARTITION (ds='2008-04-08', hr='12') PARTITION (ds='2008-04-08', hr='11');
