--! qt:disabled:Enable when we move to Calcite 1.23

set hive.optimize.constant.propagation=true;

CREATE TABLE constcasttest (id string);
INSERT INTO constcasttest values('2019-11-05 01:01:11');

set hive.cbo.enable=true;

EXPLAIN SELECT id, CAST(id AS VARCHAR(10)) FROM constcasttest WHERE CAST(id AS VARCHAR(9)) = '2019-11-0';
SELECT id, CAST(id AS VARCHAR(10)) FROM constcasttest WHERE CAST(id AS VARCHAR(9)) = '2019-11-0';

EXPLAIN SELECT id, CAST(id AS VARCHAR(10)) FROM constcasttest WHERE SUBSTR(id, 0, 9) = '2019-11-0';
SELECT id, CAST(id AS VARCHAR(10)) FROM constcasttest WHERE SUBSTR(id, 0, 9) = '2019-11-0';

set hive.cbo.enable=false;

EXPLAIN SELECT id, CAST(id AS VARCHAR(10)) FROM constcasttest WHERE CAST(id AS VARCHAR(9)) = '2019-11-0';
SELECT id, CAST(id AS VARCHAR(10)) FROM constcasttest WHERE CAST(id AS VARCHAR(9)) = '2019-11-0';

EXPLAIN SELECT id, CAST(id AS VARCHAR(10)) FROM constcasttest WHERE SUBSTR(id, 0, 9) = '2019-11-0';
SELECT id, CAST(id AS VARCHAR(10)) FROM constcasttest WHERE SUBSTR(id, 0, 9) = '2019-11-0';

