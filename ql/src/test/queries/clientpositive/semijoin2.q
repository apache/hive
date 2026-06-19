CREATE TABLE table_1_n0 (boolean_col_1 BOOLEAN, float_col_2 FLOAT, bigint_col_3 BIGINT, varchar0111_col_4 VARCHAR(111), bigint_col_5 BIGINT, float_col_6 FLOAT, boolean_col_7 BOOLEAN, decimal0101_col_8 DECIMAL(1, 1), decimal0904_col_9 DECIMAL(9, 4), char0112_col_10 CHAR(112), double_col_11 DOUBLE, boolean_col_12 BOOLEAN, double_col_13 DOUBLE, varchar0142_col_14 VARCHAR(142), timestamp_col_15 TIMESTAMP, decimal0502_col_16 DECIMAL(5, 2), smallint_col_25 SMALLINT, decimal3222_col_18 DECIMAL(32, 22), boolean_col_19 BOOLEAN, decimal2012_col_20 DECIMAL(20, 12), char0204_col_21 CHAR(204), double_col_61 DOUBLE, timestamp_col_23 TIMESTAMP, int_col_24 INT, float_col_25 FLOAT, smallint_col_26 SMALLINT, double_col_27 DOUBLE, char0180_col_28 CHAR(180), decimal1503_col_29 DECIMAL(15, 3), timestamp_col_30 TIMESTAMP, smallint_col_31 SMALLINT, decimal2020_col_32 DECIMAL(20, 20), timestamp_col_33 TIMESTAMP, boolean_col_34 BOOLEAN, decimal3025_col_35 DECIMAL(30, 25), decimal3117_col_36 DECIMAL(31, 17), timestamp_col_37 TIMESTAMP, varchar0146_col_38 VARCHAR(146), boolean_col_39 BOOLEAN, double_col_40 DOUBLE, float_col_41 FLOAT, timestamp_col_42 TIMESTAMP, double_col_43 DOUBLE, boolean_col_44 BOOLEAN, timestamp_col_45 TIMESTAMP, tinyint_col_8 TINYINT, int_col_47 INT, decimal0401_col_48 DECIMAL(4, 1), varchar0064_col_49 VARCHAR(64), string_col_50 STRING, double_col_51 DOUBLE, string_col_52 STRING, boolean_col_53 BOOLEAN, int_col_54 INT, boolean_col_55 BOOLEAN, string_col_56 STRING, double_col_57 DOUBLE, varchar0131_col_58 VARCHAR(131), boolean_col_59 BOOLEAN, bigint_col_22 BIGINT, char0184_col_61 CHAR(184), varchar0173_col_62 VARCHAR(173), timestamp_col_63 TIMESTAMP, decimal1709_col_26 DECIMAL(20, 5), timestamp_col_65 TIMESTAMP, timestamp_col_66 TIMESTAMP, timestamp_col_67 TIMESTAMP, boolean_col_68 BOOLEAN, decimal1208_col_20 DECIMAL(33, 11), decimal1605_col_70 DECIMAL(16, 5), varchar0010_col_71 VARCHAR(10), tinyint_col_72 TINYINT, timestamp_col_10 TIMESTAMP, decimal2714_col_74 DECIMAL(27, 14), double_col_75 DOUBLE, boolean_col_76 BOOLEAN, double_col_77 DOUBLE, string_col_78 STRING, boolean_col_79 BOOLEAN, boolean_col_80 BOOLEAN, decimal0803_col_81 DECIMAL(8, 3), decimal1303_col_82 DECIMAL(13, 3), tinyint_col_83 TINYINT, decimal3424_col_84 DECIMAL(34, 24), float_col_85 FLOAT, boolean_col_86 BOOLEAN, char0233_col_87 CHAR(233));

CREATE TABLE table_18_n0 (timestamp_col_1 TIMESTAMP, double_col_2 DOUBLE, boolean_col_3 BOOLEAN, timestamp_col_4 TIMESTAMP, decimal2103_col_5 DECIMAL(21, 3), char0221_col_6 CHAR(221), tinyint_col_7 TINYINT, float_col_8 FLOAT, int_col_2 INT, timestamp_col_10 TIMESTAMP, char0228_col_11 CHAR(228), timestamp_col_12 TIMESTAMP, double_col_13 DOUBLE, tinyint_col_6 TINYINT, tinyint_col_33 TINYINT, smallint_col_38 SMALLINT, boolean_col_17 BOOLEAN, double_col_18 DOUBLE, boolean_col_19 BOOLEAN, bigint_col_20 BIGINT, decimal0504_col_37 DECIMAL(37, 34), boolean_col_22 BOOLEAN, double_col_23 DOUBLE, timestamp_col_24 TIMESTAMP, varchar0076_col_25 VARCHAR(76), timestamp_col_18 TIMESTAMP, boolean_col_27 BOOLEAN, decimal1611_col_22 DECIMAL(37, 5), boolean_col_29 BOOLEAN);

set hive.cbo.enable = false;

explain
SELECT
COALESCE(498, LEAD(COALESCE(-973, -684, 515)) OVER (PARTITION BY (t2.int_col_2 + t1.smallint_col_25) ORDER BY (t2.int_col_2 + t1.smallint_col_25), FLOOR(t1.double_col_61) DESC), 524) AS int_col,
(t2.int_col_2) + (t1.smallint_col_25) AS int_col_1,
FLOOR(t1.double_col_61) AS float_col,
COALESCE(SUM(COALESCE(62, -380, -435)) OVER (PARTITION BY (t2.int_col_2 + t1.smallint_col_25) ORDER BY (t2.int_col_2 + t1.smallint_col_25) DESC, FLOOR(t1.double_col_61) DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 48 FOLLOWING), 704) AS int_col_2
FROM table_1_n0 t1
INNER JOIN table_18_n0 t2 ON (((t2.tinyint_col_6) = (t1.bigint_col_22)) AND ((t2.decimal0504_col_37) = (t1.decimal1709_col_26))) AND ((t2.tinyint_col_33) = (t1.tinyint_col_8))
WHERE
(t2.smallint_col_38) IN (SELECT
COALESCE(-92, -994) AS int_col
FROM table_1_n0 tt1
INNER JOIN table_18_n0 tt2 ON (tt2.decimal1611_col_22) = (tt1.decimal1208_col_20)
WHERE
(t1.timestamp_col_10) = (tt2.timestamp_col_18));
