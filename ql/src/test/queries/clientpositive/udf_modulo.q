--! qt:dataset:src
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION mod;
DESCRIBE FUNCTION EXTENDED mod;

SELECT mod(3, 2) FROM SRC tablesample (1 rows);

DESCRIBE FUNCTION %;
DESCRIBE FUNCTION EXTENDED %;
