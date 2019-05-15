--! qt:dataset:src
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION reflect;
DESCRIBE FUNCTION EXTENDED reflect;

EXPLAIN EXTENDED
SELECT reflect("java.lang.String", "valueOf", 1),
       reflect("java.lang.String", "isEmpty"),
       reflect("java.lang.Math", "max", 2, 3),
       reflect("java.lang.Math", "min", 2, 3),
       reflect("java.lang.Math", "round", 2.5D),
       round(reflect("java.lang.Math", "exp", 1.0D), 6),
       reflect("java.lang.Math", "floor", 1.9D),
       reflect("java.lang.Integer", "valueOf", key, 16),
       reflect("java.lang.Integer", "valueOf", "16")
FROM src tablesample (1 rows);


SELECT reflect("java.lang.String", "valueOf", 1),
       reflect("java.lang.String", "isEmpty"),
       reflect("java.lang.Math", "max", 2, 3),
       reflect("java.lang.Math", "min", 2, 3),
       reflect("java.lang.Math", "round", 2.5D),
       round(reflect("java.lang.Math", "exp", 1.0D), 6),
       reflect("java.lang.Math", "floor", 1.9D),
       reflect("java.lang.Integer", "valueOf", key, 16),
       reflect("java.lang.Integer", "valueOf", "16")
FROM src tablesample (1 rows);
