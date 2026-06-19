--! qt:dataset:src
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION position;
DESCRIBE FUNCTION EXTENDED position;

EXPLAIN
SELECT position('abc', 'abcd'),
       position('ccc', 'abcabc'),
       position('23', 123),
       position(23, 123),
       position('abc', 'abcabc', 2),
       position('abc', 'abcabc', '2'),
       position(1, TRUE),
       position(1, FALSE),
       position(CAST('2' AS TINYINT), '12345'),
       position('34', CAST('12345' AS SMALLINT)),
       position('456', CAST('123456789012' AS BIGINT)),
       position('.25', CAST(1.25 AS FLOAT)),
       position('.0', CAST(16.0 AS DOUBLE)),
       position(null, 'abc'),
       position('abc', null),
       position('abc', 'abcd', null),
       position('abc', 'abcd', 'invalid number')
FROM src tablesample (1 rows);

SELECT position('abc', 'abcd'),
       position('ccc', 'abcabc'),
       position('23', 123),
       position(23, 123),
       position('abc', 'abcabc', 2),
       position('abc', 'abcabc', '2'),
       position(1, TRUE),
       position(1, FALSE),
       position(CAST('2' AS TINYINT), '12345'),
       position('34', CAST('12345' AS SMALLINT)),
       position('456', CAST('123456789012' AS BIGINT)),
       position('.25', CAST(1.25 AS FLOAT)),
       position('.0', CAST(16.0 AS DOUBLE)),
       position(null, 'abc'),
       position('abc', null),
       position('abc', 'abcd', null),
       position('abc', 'abcd', 'invalid number')
FROM src tablesample (1 rows);
