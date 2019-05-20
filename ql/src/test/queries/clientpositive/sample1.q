--! qt:dataset:srcpart
--! qt:dataset:srcbucket
CREATE TABLE dest1_n89(key INT, value STRING, dt STRING, hr STRING) STORED AS TEXTFILE;

-- no input pruning, no sample filter
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest1_n89 SELECT s.*
FROM srcpart TABLESAMPLE (BUCKET 1 OUT OF 1 ON rand()) s
WHERE s.ds='2008-04-08' and s.hr='11';

INSERT OVERWRITE TABLE dest1_n89 SELECT s.*
FROM srcpart TABLESAMPLE (BUCKET 1 OUT OF 1 ON rand()) s
WHERE s.ds='2008-04-08' and s.hr='11';

SELECT dest1_n89.* FROM dest1_n89;

select count(1) from srcbucket;
