--! qt:dataset:src
set hive.mapred.mode=nonstrict;
-- union case: both subqueries are map jobs on same input, followed by filesink

EXPLAIN
FROM (
  FROM src select src.key, src.value WHERE src.key < 100
  UNION ALL
  FROM src SELECT src.* WHERE src.key > 100
) unioninput
INSERT OVERWRITE DIRECTORY 'target/warehouse/union.out' SELECT unioninput.* ORDER BY key, value;

FROM (
  FROM src select src.key, src.value WHERE src.key < 100
  UNION ALL
  FROM src SELECT src.* WHERE src.key > 100
) unioninput
INSERT OVERWRITE DIRECTORY 'target/warehouse/union.out' SELECT unioninput.* ORDER BY key, value;

dfs -cat ${system:test.warehouse.dir}/union.out/*;
