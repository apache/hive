--! qt:dataset:srcpart
set hive.archive.enabled = true;
-- Tests archiving a table

ALTER TABLE srcpart ARCHIVE;
