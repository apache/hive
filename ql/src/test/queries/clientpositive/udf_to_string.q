--! qt:dataset:src
set hive.fetch.task.conversion=more;

-- Conversion of main primitive types to String type:
SELECT CAST(NULL AS STRING) FROM src tablesample (1 rows);

SELECT CAST(TRUE AS STRING) FROM src tablesample (1 rows);

SELECT CAST(CAST(1 AS TINYINT) AS STRING) FROM src tablesample (1 rows);
SELECT CAST(CAST(-18 AS SMALLINT) AS STRING) FROM src tablesample (1 rows);
SELECT CAST(-129 AS STRING) FROM src tablesample (1 rows);
SELECT CAST(CAST(-1025 AS BIGINT) AS STRING) FROM src tablesample (1 rows);

SELECT CAST(CAST(-3.14 AS DOUBLE) AS STRING) FROM src tablesample (1 rows);
SELECT CAST(CAST(-3.14 AS FLOAT) AS STRING) FROM src tablesample (1 rows);
SELECT CAST(CAST(-3.14 AS DECIMAL(3,2)) AS STRING) FROM src tablesample (1 rows);

SELECT CAST('Foo' AS STRING) FROM src tablesample (1 rows);

SELECT CAST(from_utc_timestamp(timestamp '2018-05-02 15:30:30', 'PST') - from_utc_timestamp(timestamp '1970-01-30 16:00:00', 'PST') AS STRING);
SELECT CAST(interval_year_month('1-2') AS STRING);
