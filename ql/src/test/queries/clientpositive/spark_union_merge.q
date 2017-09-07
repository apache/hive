set hive.mapred.mode=nonstrict;
-- union case: both subqueries are map jobs on same input, followed by filesink
-- mostly copied from union.q

set hive.merge.sparkfiles=false;

EXPLAIN EXTENDED
FROM (
  FROM src select src.key, src.value WHERE src.key < 100
  UNION ALL
  FROM src SELECT src.* WHERE src.key > 100
) unioninput
INSERT OVERWRITE DIRECTORY 'target/warehouse/union_merge.out' SELECT unioninput.*;

FROM (
  FROM src select src.key, src.value WHERE src.key < 100
  UNION ALL
  FROM src SELECT src.* WHERE src.key > 100
) unioninput
INSERT OVERWRITE DIRECTORY 'target/warehouse/union_merge.out' SELECT unioninput.*;

dfs -ls ${system:test.warehouse.dir}/union_merge.out;

set hive.merge.sparkfiles=true;

EXPLAIN EXTENDED
FROM (
  FROM src select src.key, src.value WHERE src.key < 100
  UNION ALL
  FROM src SELECT src.* WHERE src.key > 100
) unioninput
INSERT OVERWRITE DIRECTORY 'target/warehouse/union_merge.out' SELECT unioninput.*;

FROM (
  FROM src select src.key, src.value WHERE src.key < 100
  UNION ALL
  FROM src SELECT src.* WHERE src.key > 100
) unioninput
INSERT OVERWRITE DIRECTORY 'target/warehouse/union_merge.out' SELECT unioninput.*;

dfs -ls ${system:test.warehouse.dir}/union_merge.out;
