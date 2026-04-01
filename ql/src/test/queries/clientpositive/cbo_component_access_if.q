-- HIVE-29538: AssertionError in StatefulFunctionsChecker when Calcite introduces COMPONENT_ACCESS

-- Single-level nesting: array-of-struct field projection uses COMPONENT_ACCESS.
CREATE TABLE cbo_component_access_if_tbl (
  `jobs` array<struct<code:string>>
) STORED AS ORC;

-- `if(...)` is rewritten to CASE and triggers checkForStatefulFunctions.
EXPLAIN CBO
SELECT if(concat_ws(',', `jobs`.code) = '', null, concat_ws(',', `jobs`.code)) AS codes
FROM cbo_component_access_if_tbl;
