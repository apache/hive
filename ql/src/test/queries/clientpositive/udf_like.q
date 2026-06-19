--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION like;
DESCRIBE FUNCTION EXTENDED like;

EXPLAIN
SELECT '_%_' LIKE '%\_\%\_%', '__' LIKE '%\_\%\_%', '%%_%_' LIKE '%\_\%\_%', '%_%_%' LIKE '%\%\_\%',
  '_%_' LIKE '\%\_%', '%__' LIKE '__\%%', '_%' LIKE '\_\%\_\%%', '_%' LIKE '\_\%_%',
  '%_' LIKE '\%\_', 'ab' LIKE '\%\_', 'ab' LIKE '_a%', 'ab' LIKE 'a','ab' LIKE '','' LIKE ''
FROM src WHERE src.key = 86;

SELECT '_%_' LIKE '%\_\%\_%', '__' LIKE '%\_\%\_%', '%%_%_' LIKE '%\_\%\_%', '%_%_%' LIKE '%\%\_\%',
  '_%_' LIKE '\%\_%', '%__' LIKE '__\%%', '_%' LIKE '\_\%\_\%%', '_%' LIKE '\_\%_%',
  '%_' LIKE '\%\_', 'ab' LIKE '\%\_', 'ab' LIKE '_a%', 'ab' LIKE 'a','ab' LIKE '','' LIKE ''
FROM src WHERE src.key = 86;


SELECT '1+2' LIKE '_+_', 
       '1+2' LIKE '1+_',
       '112' LIKE '1+_',
       '|||' LIKE '|_|', 
       '+++' LIKE '1+_' 
FROM src tablesample (1 rows);


CREATE TEMPORARY TABLE SplitLines(`id` string) STORED AS ORC;
INSERT INTO SplitLines SELECT 'withdraw\ncash';
SELECT `id` LIKE '%withdraw%cash' FROM SplitLines ;

CREATE TABLE SplitLinesUnderscore (q STRING) STORED AS ORC;
INSERT INTO SplitLinesUnderscore
  SELECT 'first\nsecond' UNION ALL SELECT 'first_second\nthird';
SELECT count(*) FROM SplitLinesUnderscore WHERE q LIKE '%first_second%';

-- Repeat with vectorization off to ensure consistency either way
set hive.vectorized.execution.enabled=false;

DESCRIBE FUNCTION like;
DESCRIBE FUNCTION EXTENDED like;

EXPLAIN
SELECT '_%_' LIKE '%\_\%\_%', '__' LIKE '%\_\%\_%', '%%_%_' LIKE '%\_\%\_%', '%_%_%' LIKE '%\%\_\%',
  '_%_' LIKE '\%\_%', '%__' LIKE '__\%%', '_%' LIKE '\_\%\_\%%', '_%' LIKE '\_\%_%',
  '%_' LIKE '\%\_', 'ab' LIKE '\%\_', 'ab' LIKE '_a%', 'ab' LIKE 'a','ab' LIKE '','' LIKE ''
FROM src WHERE src.key = 86;

SELECT '_%_' LIKE '%\_\%\_%', '__' LIKE '%\_\%\_%', '%%_%_' LIKE '%\_\%\_%', '%_%_%' LIKE '%\%\_\%',
  '_%_' LIKE '\%\_%', '%__' LIKE '__\%%', '_%' LIKE '\_\%\_\%%', '_%' LIKE '\_\%_%',
  '%_' LIKE '\%\_', 'ab' LIKE '\%\_', 'ab' LIKE '_a%', 'ab' LIKE 'a','ab' LIKE '','' LIKE ''
FROM src WHERE src.key = 86;


SELECT '1+2' LIKE '_+_',
       '1+2' LIKE '1+_',
       '112' LIKE '1+_',
       '|||' LIKE '|_|',
       '+++' LIKE '1+_'
FROM src tablesample (1 rows);

SELECT `id` LIKE '%withdraw%cash' FROM SplitLines;
SELECT count(*) FROM SplitLinesUnderscore WHERE q LIKE '%first_second%';
