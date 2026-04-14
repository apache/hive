-- SORT_QUERY_RESULTS
SET hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
SET hive.fetch.task.conversion=none;

CREATE TABLE test_parquet_array_nulls_bool (
    id INT,
    arr_prim ARRAY<BOOLEAN>
) STORED AS PARQUET;

INSERT INTO test_parquet_array_nulls_bool VALUES
    (1, array(CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN))),
    (2, if(1=0, array(true, false), null)),
    (3, array(true, CAST(NULL AS BOOLEAN))),
    (4, array(true, false));

SELECT * FROM test_parquet_array_nulls_bool;

CREATE TABLE test_parquet_array_nulls_double (
    id INT,
    arr_prim ARRAY<DOUBLE>
) STORED AS PARQUET;

INSERT INTO test_parquet_array_nulls_double
SELECT 1, array(CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE))
UNION ALL
SELECT 2, CAST(NULL AS ARRAY<DOUBLE>)
UNION ALL
SELECT 3, array(CAST(3.3 AS DOUBLE), CAST(NULL AS DOUBLE))
UNION ALL
SELECT 4, array(CAST(4.4 AS DOUBLE), CAST(5.5 AS DOUBLE));

SELECT * FROM test_parquet_array_nulls_double;

CREATE TABLE test_parquet_array_nulls_varchar (
    id INT,
    arr_prim ARRAY<VARCHAR(20)>
) STORED AS PARQUET;

INSERT INTO test_parquet_array_nulls_varchar
SELECT 1, array(CAST(NULL AS VARCHAR(20)), CAST(NULL AS VARCHAR(20)));
INSERT INTO test_parquet_array_nulls_varchar
SELECT 2, CAST(NULL AS ARRAY<VARCHAR(20)>);

SELECT * FROM test_parquet_array_nulls_varchar;

CREATE TABLE test_parquet_array_nulls_float (
    id INT,
    arr_prim ARRAY<FLOAT>
) STORED AS PARQUET;

INSERT INTO test_parquet_array_nulls_float
SELECT 1, array(CAST(NULL AS FLOAT), CAST(NULL AS FLOAT))
UNION ALL
SELECT 2, CAST(NULL AS ARRAY<FLOAT>)
UNION ALL
SELECT 3, array(CAST(3.3 AS FLOAT), CAST(NULL AS FLOAT))
UNION ALL
SELECT 4, array(CAST(4.4 AS FLOAT), CAST(5.5 AS FLOAT));

SELECT * FROM test_parquet_array_nulls_float;

SET hive.vectorized.execution.enabled=false;
SELECT * FROM test_parquet_array_nulls_bool;
SELECT * FROM test_parquet_array_nulls_double;
SELECT * FROM test_parquet_array_nulls_varchar;
SELECT * FROM test_parquet_array_nulls_float;
