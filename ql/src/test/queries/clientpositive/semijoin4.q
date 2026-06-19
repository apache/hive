set hive.mapred.mode=nonstrict;

CREATE TABLE table_1 (int_col_1 INT, decimal3003_col_2 DECIMAL(30, 3), timestamp_col_3 TIMESTAMP, decimal0101_col_4 DECIMAL(1, 1), double_col_5 DOUBLE, boolean_col_6 BOOLEAN, timestamp_col_7 TIMESTAMP, varchar0098_col_8 VARCHAR(98), int_col_9 INT, timestamp_col_10 TIMESTAMP, decimal0903_col_11 DECIMAL(9, 3), int_col_12 INT, bigint_col_13 BIGINT, boolean_col_14 BOOLEAN, char0254_col_15 CHAR(254), boolean_col_16 BOOLEAN, smallint_col_17 SMALLINT, float_col_18 FLOAT, decimal2608_col_19 DECIMAL(26, 8), varchar0216_col_20 VARCHAR(216), string_col_21 STRING, timestamp_col_22 TIMESTAMP, double_col_23 DOUBLE, smallint_col_24 SMALLINT, float_col_25 FLOAT, decimal2016_col_26 DECIMAL(20, 16), string_col_27 STRING, decimal0202_col_28 DECIMAL(2, 2), boolean_col_29 BOOLEAN, decimal2020_col_30 DECIMAL(20, 20), float_col_31 FLOAT, boolean_col_32 BOOLEAN, varchar0148_col_33 VARCHAR(148), decimal2121_col_34 DECIMAL(21, 21), timestamp_col_35 TIMESTAMP, float_col_36 FLOAT, float_col_37 FLOAT, string_col_38 STRING, decimal3420_col_39 DECIMAL(34, 20), smallint_col_40 SMALLINT, decimal1408_col_41 DECIMAL(14, 8), string_col_42 STRING, decimal0902_col_43 DECIMAL(9, 2), varchar0204_col_44 VARCHAR(204), float_col_45 FLOAT, tinyint_col_46 TINYINT, double_col_47 DOUBLE, timestamp_col_48 TIMESTAMP, double_col_49 DOUBLE, timestamp_col_50 TIMESTAMP, decimal0704_col_51 DECIMAL(7, 4), int_col_52 INT, double_col_53 DOUBLE, int_col_54 INT, timestamp_col_55 TIMESTAMP, decimal0505_col_56 DECIMAL(5, 5), char0155_col_57 CHAR(155), double_col_58 DOUBLE, timestamp_col_59 TIMESTAMP, double_col_60 DOUBLE, float_col_61 FLOAT, char0249_col_62 CHAR(249), float_col_63 FLOAT, smallint_col_64 SMALLINT, decimal1309_col_65 DECIMAL(13, 9), timestamp_col_66 TIMESTAMP, boolean_col_67 BOOLEAN, tinyint_col_68 TINYINT, tinyint_col_69 TINYINT, double_col_70 DOUBLE, bigint_col_71 BIGINT, boolean_col_72 BOOLEAN, float_col_73 FLOAT, char0222_col_74 CHAR(222), boolean_col_75 BOOLEAN, string_col_76 STRING, decimal2612_col_77 DECIMAL(26, 12), bigint_col_78 BIGINT, char0128_col_79 CHAR(128), tinyint_col_80 TINYINT, boolean_col_81 BOOLEAN, int_col_82 INT, boolean_col_83 BOOLEAN, decimal2622_col_84 DECIMAL(26, 22), boolean_col_85 BOOLEAN, boolean_col_86 BOOLEAN, decimal0907_col_87 DECIMAL(9, 7))
STORED AS orc;
CREATE TABLE table_18 (float_col_1 FLOAT, double_col_2 DOUBLE, decimal2518_col_3 DECIMAL(25, 18), boolean_col_4 BOOLEAN, bigint_col_5 BIGINT, boolean_col_6 BOOLEAN, boolean_col_7 BOOLEAN, char0035_col_8 CHAR(35), decimal2709_col_9 DECIMAL(27, 9), timestamp_col_10 TIMESTAMP, bigint_col_11 BIGINT, decimal3604_col_12 DECIMAL(36, 4), string_col_13 STRING, timestamp_col_14 TIMESTAMP, timestamp_col_15 TIMESTAMP, decimal1911_col_16 DECIMAL(19, 11), boolean_col_17 BOOLEAN, tinyint_col_18 TINYINT, timestamp_col_19 TIMESTAMP, timestamp_col_20 TIMESTAMP, tinyint_col_21 TINYINT, float_col_22 FLOAT, timestamp_col_23 TIMESTAMP)
STORED AS orc;

explain
SELECT
    COALESCE(498,
      LEAD(COALESCE(-973, -684, 515)) OVER (
        PARTITION BY (t2.tinyint_col_21 + t1.smallint_col_24)
        ORDER BY (t2.tinyint_col_21 + t1.smallint_col_24),
        FLOOR(t1.double_col_60) DESC),
      524) AS int_col
FROM table_1 t1 INNER JOIN table_18 t2
ON (((t2.tinyint_col_18) = (t1.bigint_col_13))
    AND ((t2.decimal2709_col_9) = (t1.decimal1309_col_65)))
    AND ((t2.tinyint_col_21) = (t1.tinyint_col_46))
WHERE (t2.tinyint_col_21) IN (
        SELECT COALESCE(-92, -994) AS int_col_3
        FROM table_1 tt1 INNER JOIN table_18 tt2
        ON (tt2.decimal1911_col_16) = (tt1.decimal1309_col_65)
        WHERE (tt1.timestamp_col_66) = (tt2.timestamp_col_19));
