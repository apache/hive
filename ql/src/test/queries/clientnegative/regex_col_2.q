--! qt:dataset:srcpart
set hive.support.quoted.identifiers=none;
EXPLAIN
SELECT `.a.` FROM srcpart;
