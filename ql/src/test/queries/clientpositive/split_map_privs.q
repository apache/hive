--! qt:dataset:src
set hive.fetch.task.conversion=more;

use default;
DESCRIBE FUNCTION split_map_privs;
DESCRIBE FUNCTION EXTENDED split_map_privs;

EXPLAIN SELECT
  split_map_privs('1 0 0 0 0 0 0 0 0 0'),
  split_map_privs('1 0 0 1 0 0 0 0 0 0')
FROM src tablesample (1 rows);


SELECT
  split_map_privs('1 0 0 0 0 0 0 0 0 0'),
  split_map_privs('1 0 0 1 0 0 0 0 0 0')
FROM src tablesample (1 rows);
