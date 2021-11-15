CREATE TABLE table_16 (
timestamp_col_19    timestamp,
timestamp_col_29    timestamp
);

INSERT INTO table_16(timestamp_col_19, timestamp_col_29) VALUES
('2018-01-10 15:03:55.0', '2018-01-10 15:04:55.0'),
('2018-01-10 15:03:55.0', '2018-01-10 15:04:55.0'),
('2018-02-10 07:12:55.0', '2018-02-10 07:12:55.0'),
('2020-01-01 00:00:01.0', '2020-01-01 00:00:02.0');


CREATE TABLE table_7 (
int_col_10      int,
bigint_col_3    bigint
);

INSERT INTO table_7(int_col_10, bigint_col_3) VALUES
(3, 200),
(3, 100),
(2, 250),
(2, 280),
(2, 50);


CREATE TABLE table_10 (
boolean_col_16      boolean,
timestamp_col_5     timestamp,
timestamp_col_15    timestamp,
timestamp_col_30    timestamp,
int_col_18          int
);

INSERT INTO table_10(boolean_col_16, timestamp_col_5, timestamp_col_15, timestamp_col_30, int_col_18) VALUES
(true, '2018-01-10 15:03:55.0', '2018-01-10 15:03:55.0', '2018-01-10 15:03:55.0', 11),
(true, '2018-01-10 15:03:55.0', '2018-01-10 15:03:55.0', '2018-01-10 15:03:55.0', 11),
(true, '2018-01-10 15:03:55.0', '2018-01-10 15:03:55.0', '2018-01-10 15:03:55.0', 11),
(true, '2018-02-10 07:12:55.0', '2018-02-10 07:12:55.0', '2018-02-10 07:12:55.0', 15),
(true, '2018-02-10 07:12:55.0', '2018-02-10 07:12:55.0', '2018-02-10 07:12:55.0', 15),
(true, '2018-03-10 03:05:01.0', '2018-03-10 03:05:01.0', '2018-03-10 03:05:01.0', 18);


explain cbo
SELECT
    DISTINCT COALESCE(a4.timestamp_col_15, IF(a4.boolean_col_16, a4.timestamp_col_30, a4.timestamp_col_5)) AS timestamp_col
FROM table_7 a3
RIGHT JOIN table_10 a4
WHERE (a3.bigint_col_3) >= (a4.int_col_18)
INTERSECT ALL
SELECT
    COALESCE(LEAST(
        COALESCE(a1.timestamp_col_19, CAST('2010-03-29 00:00:00' AS TIMESTAMP)),
        COALESCE(a1.timestamp_col_29, CAST('2014-08-16 00:00:00' AS TIMESTAMP))
        ),
        GREATEST(COALESCE(a1.timestamp_col_19, CAST('2013-07-01 00:00:00' AS TIMESTAMP)),
        COALESCE(a1.timestamp_col_29, CAST('2028-06-18 00:00:00' AS TIMESTAMP)))
    ) AS timestamp_col
FROM table_16 a1
    GROUP BY COALESCE(LEAST(
        COALESCE(a1.timestamp_col_19, CAST('2010-03-29 00:00:00' AS TIMESTAMP)),
        COALESCE(a1.timestamp_col_29, CAST('2014-08-16 00:00:00' AS TIMESTAMP))
    ),
    GREATEST(
        COALESCE(a1.timestamp_col_19, CAST('2013-07-01 00:00:00' AS TIMESTAMP)),
        COALESCE(a1.timestamp_col_29, CAST('2028-06-18 00:00:00' AS TIMESTAMP)))
    );

SELECT
    DISTINCT COALESCE(a4.timestamp_col_15, IF(a4.boolean_col_16, a4.timestamp_col_30, a4.timestamp_col_5)) AS timestamp_col
FROM table_7 a3
RIGHT JOIN table_10 a4
WHERE (a3.bigint_col_3) >= (a4.int_col_18)
INTERSECT ALL
SELECT
    COALESCE(LEAST(
        COALESCE(a1.timestamp_col_19, CAST('2010-03-29 00:00:00' AS TIMESTAMP)),
        COALESCE(a1.timestamp_col_29, CAST('2014-08-16 00:00:00' AS TIMESTAMP))
        ),
        GREATEST(COALESCE(a1.timestamp_col_19, CAST('2013-07-01 00:00:00' AS TIMESTAMP)),
        COALESCE(a1.timestamp_col_29, CAST('2028-06-18 00:00:00' AS TIMESTAMP)))
    ) AS timestamp_col
FROM table_16 a1
    GROUP BY COALESCE(LEAST(
        COALESCE(a1.timestamp_col_19, CAST('2010-03-29 00:00:00' AS TIMESTAMP)),
        COALESCE(a1.timestamp_col_29, CAST('2014-08-16 00:00:00' AS TIMESTAMP))
    ),
    GREATEST(
        COALESCE(a1.timestamp_col_19, CAST('2013-07-01 00:00:00' AS TIMESTAMP)),
        COALESCE(a1.timestamp_col_29, CAST('2028-06-18 00:00:00' AS TIMESTAMP)))
    );
