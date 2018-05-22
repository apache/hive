SET hive.cbo.enable=false;

CREATE TABLE T1(a STRING, b STRING, s BIGINT);
INSERT OVERWRITE TABLE T1 VALUES ('aaa', 'bbb', 123456);

-- should not pushdown, otherwise the filter 'a IS NOT NULL' will take no effect
-- SORT_QUERY_RESULTS
SELECT * FROM (
    SELECT a, b, sum(s)
    FROM T1
    GROUP BY a, b GROUPING SETS ((), (a), (b), (a, b))
) t WHERE a IS NOT NULL;

-- should pushdown
-- SORT_QUERY_RESULTS
SELECT * FROM (
    SELECT a, b, sum(s)
    FROM T1
    GROUP BY a, b GROUPING SETS ((a), (a, b))
) t WHERE a IS NOT NULL;
