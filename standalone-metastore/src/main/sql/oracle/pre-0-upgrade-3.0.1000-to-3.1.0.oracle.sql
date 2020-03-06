-- This is necessary in CDH5-CDH7 upgrade path. The index already exists, but CAT_NAME column is missing from it.
-- So dropping in pre-upgrade and recreating in the upgrade script is needed.
DROP INDEX TAB_COL_STATS_IDX;