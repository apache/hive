-- SORT_QUERY_RESULTS
SET hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
SET hive.fetch.task.conversion=none;

CREATE TABLE test_parquet_struct_nulls (
    id INT,
    st_prim STRUCT<x:INT, y:INT>
) STORED AS PARQUET;

INSERT INTO test_parquet_struct_nulls VALUES
    (1, named_struct('x', CAST(NULL AS INT), 'y', CAST(NULL AS INT))),
    (2, if(1=0, named_struct('x', 1, 'y', 1), null)),
    (3, named_struct('x', 3, 'y', CAST(NULL AS INT))),
    (4, named_struct('x', 4, 'y', 4));

-- Test A: Full table scan to check JSON representation
SELECT * FROM test_parquet_struct_nulls;

-- Test B: Verify IS NULL evaluates correctly
SELECT id FROM test_parquet_struct_nulls WHERE st_prim IS NULL;

-- Test C: Verify IS NOT NULL evaluates correctly
SELECT id FROM test_parquet_struct_nulls WHERE st_prim IS NOT NULL;

-- Test D: Verify field-level null evaluation inside a valid struct
SELECT id FROM test_parquet_struct_nulls WHERE st_prim IS NOT NULL AND st_prim.x IS NULL;

-- Validate without vectorization
SET hive.vectorized.execution.enabled=false;
SELECT * FROM test_parquet_struct_nulls;