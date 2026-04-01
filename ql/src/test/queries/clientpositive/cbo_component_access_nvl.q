-- Repro for HIVE-29538 issue: AssertionError when Calcite operator COMPONENT_ACCESS

set hive.cbo.enable=true;

DROP TABLE IF EXISTS cbo_component_access_if_tbl;

CREATE TABLE cbo_component_access_if_tbl (
  `data` array<struct<jobs:array<struct<code:string,label:string>>>>
) STORED AS ORC;

INSERT INTO TABLE cbo_component_access_if_tbl
SELECT array(
  named_struct(
    'jobs', array(
      named_struct('code', 'j1', 'label', 'l1'),
      named_struct('code', 'j2', 'label', 'l2')
    )
  )
);

-- `data` is both the column name and the LATERAL VIEW table alias (matches common real-world queries).
-- Expression `data.dat.jobs.code` forces Calcite to introduce COMPONENT_ACCESS for array-of-struct field access.
-- `if(...)` is translated to CASE, and HiveFunctionHelper.checkForStatefulFunctions walks the Rex tree.
SELECT
  if(concat_ws(',', data.dat.jobs.code) = '', null, concat_ws(',', data.dat.jobs.code)) AS jobs_codes
FROM cbo_component_access_if_tbl t
LATERAL VIEW explode(t.`data`) `data` AS `dat`;
