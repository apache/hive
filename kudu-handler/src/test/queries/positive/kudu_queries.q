-- Create table specifying columns.
-- Note: Kudu is the source of truth for schema.
DROP TABLE IF EXISTS kv_table;
CREATE EXTERNAL TABLE kv_table(key int, value string)
STORED BY 'org.apache.hadoop.hive.kudu.KuduStorageHandler'
TBLPROPERTIES ("kudu.table_name" = "default.kudu_kv");

DESCRIBE EXTENDED kv_table;

-- Verify INSERT support.
INSERT INTO TABLE kv_table VALUES
(1, "1"), (2, "2");

SELECT * FROM kv_table;
SELECT count(*) FROM kv_table;
SELECT count(*) FROM kv_table LIMIT 1;
SELECT count(1) FROM kv_table;

-- Verify projection and case insensitivity.
SELECT kEy FROM kv_table;

DROP TABLE kv_table;

-- Create table without specifying columns.
-- Note: Kudu is the source of truth for schema.
DROP TABLE IF EXISTS all_types_table;
CREATE EXTERNAL TABLE all_types_table
STORED BY 'org.apache.hadoop.hive.kudu.KuduStorageHandler'
TBLPROPERTIES ("kudu.table_name" = "default.kudu_all_types");

DESCRIBE EXTENDED all_types_table;

INSERT INTO TABLE all_types_table VALUES
(1, 1, 1, 1, true, 1.1, 1.1, "one", 'one', '2001-01-01 01:11:11', 1.111, null, 1),
(2, 2, 2, 2, false, 2.2, 2.2, "two", 'two', '2001-01-01 01:12:12', 2.222, "not null", 2);

SELECT * FROM all_types_table;
SELECT count(*) FROM all_types_table;

-- Verify comparison predicates on byte.
EXPLAIN SELECT key FROM all_types_table WHERE key = 1;
SELECT key FROM all_types_table WHERE key = 1;
SELECT key FROM all_types_table WHERE key != 1;
SELECT key FROM all_types_table WHERE key > 1;
SELECT key FROM all_types_table WHERE key >= 1;
SELECT key FROM all_types_table WHERE key < 2;
SELECT key FROM all_types_table WHERE key <= 2;

-- Verify comparison predicates on short.
EXPLAIN SELECT key FROM all_types_table WHERE int16 = 1;
SELECT key FROM all_types_table WHERE int16 = 1;
SELECT key FROM all_types_table WHERE int16 != 1;
SELECT key FROM all_types_table WHERE int16 > 1;
SELECT key FROM all_types_table WHERE int16 >= 1;
SELECT key FROM all_types_table WHERE int16 < 2;
SELECT key FROM all_types_table WHERE int16 <= 2;

-- Verify comparison predicates on int.
EXPLAIN SELECT key FROM all_types_table WHERE int32 = 1;
SELECT key FROM all_types_table WHERE int32 = 1;
SELECT key FROM all_types_table WHERE int32 != 1;
SELECT key FROM all_types_table WHERE int32 > 1;
SELECT key FROM all_types_table WHERE int32 >= 1;
SELECT key FROM all_types_table WHERE int32 < 2;
SELECT key FROM all_types_table WHERE int32 <= 2;

-- Verify comparison predicates on long.
EXPLAIN SELECT key FROM all_types_table WHERE int64 = 1;
SELECT key FROM all_types_table WHERE int64 = 1;
SELECT key FROM all_types_table WHERE int64 != 1;
SELECT key FROM all_types_table WHERE int64 > 1;
SELECT key FROM all_types_table WHERE int64 >= 1;
SELECT key FROM all_types_table WHERE int64 < 2;
SELECT key FROM all_types_table WHERE int64 <= 2;

-- Verify comparison predicates on boolean.
EXPLAIN SELECT key FROM all_types_table WHERE bool = true;
SELECT key FROM all_types_table WHERE bool = true;
SELECT key FROM all_types_table WHERE bool != true;
SELECT key FROM all_types_table WHERE bool > true;
SELECT key FROM all_types_table WHERE bool >= true;
SELECT key FROM all_types_table WHERE bool < false;
SELECT key FROM all_types_table WHERE bool <= false;

-- Verify comparison predicates on string.
-- Note: string is escaped because it's a reserved word.
EXPLAIN SELECT key FROM all_types_table WHERE `string` = "one";
SELECT key FROM all_types_table WHERE `string` = "one";
SELECT key FROM all_types_table WHERE `string` != "one";
SELECT key FROM all_types_table WHERE `string` > "one";
SELECT key FROM all_types_table WHERE `string` >= "one";
SELECT key FROM all_types_table WHERE `string` < "two";
SELECT key FROM all_types_table WHERE `string` <= "two";

-- Verify comparison predicates on binary.
-- Note: binary is escaped because it's a reserved word.
EXPLAIN SELECT key FROM all_types_table WHERE `binary` = cast ('one' as binary);
SELECT key FROM all_types_table WHERE `binary` = cast ('one' as binary);
SELECT key FROM all_types_table WHERE `binary` != cast ('one' as binary);
SELECT key FROM all_types_table WHERE `binary` > cast ('one' as binary);
SELECT key FROM all_types_table WHERE `binary` >= cast ('one' as binary);
SELECT key FROM all_types_table WHERE `binary` < cast ('two' as binary);
SELECT key FROM all_types_table WHERE `binary` <= cast ('two' as binary);

-- Verify comparison predicates on timestamp.
-- Note: timestamp is escaped because it's a reserved word.
EXPLAIN SELECT key FROM all_types_table WHERE `timestamp` = '2001-01-01 01:11:11';
SELECT key FROM all_types_table WHERE `timestamp` = '2001-01-01 01:11:11';
SELECT key FROM all_types_table WHERE `timestamp` != '2001-01-01 01:11:11';
SELECT key FROM all_types_table WHERE `timestamp` > '2001-01-01 01:11:11';
SELECT key FROM all_types_table WHERE `timestamp` >= '2001-01-01 01:11:11';
SELECT key FROM all_types_table WHERE `timestamp` < '2001-01-01 01:12:12';
SELECT key FROM all_types_table WHERE `timestamp` <= '2001-01-01 01:12:12';

-- Verify comparison predicates on float.
-- Note: float is escaped because it's a reserved word.
EXPLAIN SELECT key FROM all_types_table WHERE `float` = 1.1;
SELECT key FROM all_types_table WHERE `float` = 1.1;
SELECT key FROM all_types_table WHERE `float` != 1.1;
SELECT key FROM all_types_table WHERE `float` > 1.1;
SELECT key FROM all_types_table WHERE `float` >= 1.1;
SELECT key FROM all_types_table WHERE `float` < 2.2;
SELECT key FROM all_types_table WHERE `float` <= 2.2;

-- Verify comparison predicates on double.
-- Note: double is escaped because it's a reserved word.
EXPLAIN SELECT key FROM all_types_table WHERE `double` = 1.1;
SELECT key FROM all_types_table WHERE `double` = 1.1;
SELECT key FROM all_types_table WHERE `double` != 1.1;
SELECT key FROM all_types_table WHERE `double` > 1.1;
SELECT key FROM all_types_table WHERE `double` >= 1.1;
SELECT key FROM all_types_table WHERE `double` < 2.2;
SELECT key FROM all_types_table WHERE `double` <= 2.2;

-- Verify comparison predicates on decimal.
-- Note: decimal is escaped because it's a reserved word.
EXPLAIN SELECT key FROM all_types_table WHERE `decimal` = 1.111;
SELECT key FROM all_types_table WHERE `decimal` = 1.111;
SELECT key FROM all_types_table WHERE `decimal` != 1.111;
SELECT key FROM all_types_table WHERE `decimal` > 1.111;
SELECT key FROM all_types_table WHERE `decimal` >= 1.111;
SELECT key FROM all_types_table WHERE `decimal` < 2.222;
SELECT key FROM all_types_table WHERE `decimal` <= 2.222;

-- Verify null predicates.
-- Note: null is escaped because it's a reserved word.
EXPLAIN SELECT key FROM all_types_table WHERE `null` IS NOT NULL;
SELECT key FROM all_types_table WHERE `null` IS NOT NULL;
SELECT key FROM all_types_table WHERE `null` IS NULL;
SELECT key FROM all_types_table WHERE `null` = NULL;
SELECT key FROM all_types_table WHERE `null` <=> NULL;

-- Verify AND predicates.
EXPLAIN SELECT key FROM all_types_table WHERE `null` IS NULL AND key < 2;
SELECT key FROM all_types_table WHERE `null` IS NULL AND key < 2;

-- Verify OR predicates.
EXPLAIN SELECT key FROM all_types_table WHERE key = 1 OR `string` = "one";
SELECT key FROM all_types_table WHERE key = 1 OR `string` = "one";
SELECT key FROM all_types_table WHERE key = 1 OR `string` = "two";

-- Verify various other filters.
SELECT key FROM all_types_table WHERE string IN ("one", "missing");
SELECT key FROM all_types_table WHERE string NOT IN ("one", "missing");
SELECT key FROM all_types_table WHERE key BETWEEN -1 and 1;
SELECT key FROM all_types_table WHERE `string` LIKE "%n%";

-- Verify statistics
ANALYZE TABLE all_types_table COMPUTE STATISTICS FOR COLUMNS;
EXPLAIN SELECT * FROM all_types_table;

-- Verify show and describe capabilities.
SHOW CREATE TABLE all_types_table;
DESCRIBE all_types_table;
DESCRIBE FORMATTED all_types_table;

DROP TABLE all_types_table;