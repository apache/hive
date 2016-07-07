set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION java_method;
DESCRIBE FUNCTION EXTENDED java_method;

-- java_method() is a synonym for reflect()

EXPLAIN EXTENDED
SELECT java_method("java.lang.String", "valueOf", 1),
       java_method("java.lang.String", "isEmpty"),
       java_method("java.lang.Math", "max", 2, 3),
       java_method("java.lang.Math", "min", 2, 3),
       java_method("java.lang.Math", "round", 2.5D),
       round(java_method("java.lang.Math", "exp", 1.0D), 6),
       java_method("java.lang.Math", "floor", 1.9D)
FROM src tablesample (1 rows);


SELECT java_method("java.lang.String", "valueOf", 1),
       java_method("java.lang.String", "isEmpty"),
       java_method("java.lang.Math", "max", 2, 3),
       java_method("java.lang.Math", "min", 2, 3),
       java_method("java.lang.Math", "round", 2.5D),
       round(java_method("java.lang.Math", "exp", 1.0D), 6),
       java_method("java.lang.Math", "floor", 1.9D)
FROM src tablesample (1 rows);

