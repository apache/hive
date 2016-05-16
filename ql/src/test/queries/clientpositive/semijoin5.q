CREATE TABLE table_1 (timestamp_col_1 TIMESTAMP, decimal3003_col_2 DECIMAL(30, 3), tinyint_col_3 TINYINT, decimal0101_col_4 DECIMAL(1, 1), boolean_col_5 BOOLEAN, float_col_6 FLOAT, bigint_col_7 BIGINT, varchar0098_col_8 VARCHAR(98), timestamp_col_9 TIMESTAMP, bigint_col_10 BIGINT, decimal0903_col_11 DECIMAL(9, 3), timestamp_col_12 TIMESTAMP, timestamp_col_13 TIMESTAMP, float_col_14 FLOAT, char0254_col_15 CHAR(254), double_col_16 DOUBLE, timestamp_col_17 TIMESTAMP, boolean_col_18 BOOLEAN, decimal2608_col_19 DECIMAL(26, 8), varchar0216_col_20 VARCHAR(216), string_col_21 STRING, bigint_col_22 BIGINT, boolean_col_23 BOOLEAN, timestamp_col_24 TIMESTAMP, boolean_col_25 BOOLEAN, decimal2016_col_26 DECIMAL(20, 16), string_col_27 STRING, decimal0202_col_28 DECIMAL(2, 2), float_col_29 FLOAT, decimal2020_col_30 DECIMAL(20, 20), boolean_col_31 BOOLEAN, double_col_32 DOUBLE, varchar0148_col_33 VARCHAR(148), decimal2121_col_34 DECIMAL(21, 21), tinyint_col_35 TINYINT, boolean_col_36 BOOLEAN, boolean_col_37 BOOLEAN, string_col_38 STRING, decimal3420_col_39 DECIMAL(34, 20), timestamp_col_40 TIMESTAMP, decimal1408_col_41 DECIMAL(14, 8), string_col_42 STRING, decimal0902_col_43 DECIMAL(9, 2), varchar0204_col_44 VARCHAR(204), boolean_col_45 BOOLEAN, timestamp_col_46 TIMESTAMP, boolean_col_47 BOOLEAN, bigint_col_48 BIGINT, boolean_col_49 BOOLEAN, smallint_col_50 SMALLINT, decimal0704_col_51 DECIMAL(7, 4), timestamp_col_52 TIMESTAMP, boolean_col_53 BOOLEAN, timestamp_col_54 TIMESTAMP, int_col_55 INT, decimal0505_col_56 DECIMAL(5, 5), char0155_col_57 CHAR(155), boolean_col_58 BOOLEAN, bigint_col_59 BIGINT, boolean_col_60 BOOLEAN, boolean_col_61 BOOLEAN, char0249_col_62 CHAR(249), boolean_col_63 BOOLEAN, timestamp_col_64 TIMESTAMP, decimal1309_col_65 DECIMAL(13, 9), int_col_66 INT, float_col_67 FLOAT, timestamp_col_68 TIMESTAMP, timestamp_col_69 TIMESTAMP, boolean_col_70 BOOLEAN, timestamp_col_71 TIMESTAMP, double_col_72 DOUBLE, boolean_col_73 BOOLEAN, char0222_col_74 CHAR(222), float_col_75 FLOAT, string_col_76 STRING, decimal2612_col_77 DECIMAL(26, 12), timestamp_col_78 TIMESTAMP, char0128_col_79 CHAR(128), timestamp_col_80 TIMESTAMP, double_col_81 DOUBLE, timestamp_col_82 TIMESTAMP, float_col_83 FLOAT, decimal2622_col_84 DECIMAL(26, 22), double_col_85 DOUBLE, float_col_86 FLOAT, decimal0907_col_87 DECIMAL(9, 7)) STORED AS orc;

CREATE TABLE table_18 (boolean_col_1 BOOLEAN, boolean_col_2 BOOLEAN, decimal2518_col_3 DECIMAL(25, 18), float_col_4 FLOAT, timestamp_col_5 TIMESTAMP, double_col_6 DOUBLE, double_col_7 DOUBLE, char0035_col_8 CHAR(35), decimal2709_col_9 DECIMAL(27, 9), int_col_10 INT, timestamp_col_11 TIMESTAMP, decimal3604_col_12 DECIMAL(36, 4), string_col_13 STRING, int_col_14 INT, tinyint_col_15 TINYINT, decimal1911_col_16 DECIMAL(19, 11), float_col_17 FLOAT, timestamp_col_18 TIMESTAMP, smallint_col_19 SMALLINT, tinyint_col_20 TINYINT, timestamp_col_21 TIMESTAMP, boolean_col_22 BOOLEAN, int_col_23 INT) STORED AS orc;

explain
SELECT
    COALESCE(498, LEAD(COALESCE(-973, -684, 515)) OVER (PARTITION BY (t2.int_col_10 + t1.smallint_col_50) ORDER BY (t2.int_col_10 + t1.smallint_col_50), FLOOR(t1.double_col_16) DESC), 524) AS int_col,
    (t2.int_col_10) + (t1.smallint_col_50) AS int_col_1,
    FLOOR(t1.double_col_16) AS float_col,
    COALESCE(SUM(COALESCE(62, -380, -435)) OVER (PARTITION BY (t2.int_col_10 + t1.smallint_col_50) ORDER BY (t2.int_col_10 + t1.smallint_col_50) DESC, FLOOR(t1.double_col_16) DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 48 FOLLOWING), 704) AS int_col_2
FROM table_1 t1
INNER JOIN table_18 t2 ON (((t2.tinyint_col_15) = (t1.bigint_col_7)) AND
                           ((t2.decimal2709_col_9) = (t1.decimal2016_col_26))) AND
                           ((t2.tinyint_col_20) = (t1.tinyint_col_3))
WHERE (t2.smallint_col_19) IN (SELECT
    COALESCE(-92, -994) AS int_col
    FROM table_1 tt1
    INNER JOIN table_18 tt2 ON (tt2.decimal1911_col_16) = (tt1.decimal2612_col_77)
    WHERE (t1.timestamp_col_9) = (tt2.timestamp_col_18));

SELECT
    COALESCE(498, LEAD(COALESCE(-973, -684, 515)) OVER (PARTITION BY (t2.int_col_10 + t1.smallint_col_50) ORDER BY (t2.int_col_10 + t1.smallint_col_50), FLOOR(t1.double_col_16) DESC), 524) AS int_col,
    (t2.int_col_10) + (t1.smallint_col_50) AS int_col_1,
    FLOOR(t1.double_col_16) AS float_col,
    COALESCE(SUM(COALESCE(62, -380, -435)) OVER (PARTITION BY (t2.int_col_10 + t1.smallint_col_50) ORDER BY (t2.int_col_10 + t1.smallint_col_50) DESC, FLOOR(t1.double_col_16) DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 48 FOLLOWING), 704) AS int_col_2
FROM table_1 t1
INNER JOIN table_18 t2 ON (((t2.tinyint_col_15) = (t1.bigint_col_7)) AND
                           ((t2.decimal2709_col_9) = (t1.decimal2016_col_26))) AND
                           ((t2.tinyint_col_20) = (t1.tinyint_col_3))
WHERE (t2.smallint_col_19) IN (SELECT
    COALESCE(-92, -994) AS int_col
    FROM table_1 tt1
    INNER JOIN table_18 tt2 ON (tt2.decimal1911_col_16) = (tt1.decimal2612_col_77)
    WHERE (t1.timestamp_col_9) = (tt2.timestamp_col_18));

