set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION least;
DESCRIBE FUNCTION EXTENDED least;

EXPLAIN
SELECT LEAST('a', 'b', 'c'),
       LEAST('C', 'a', 'B'),
       LEAST('AAA', 'AaA', 'AAa'),
       LEAST('A', 'AA', 'AAA'),
       LEAST('11', '13', '12'),
       LEAST('11', '2', '12'),
       LEAST('01', '03', '02'),
       LEAST('01', '1', '02'),
       LEAST(null, 'b', 'c' ),
       LEAST('a', null, 'c'),
       LEAST('a', 'b', null ),
       LEAST('a', null, null),
       LEAST(null, 'b', null),
       LEAST(cast(null as string), null, null)
FROM src tablesample (1 rows);

SELECT LEAST('a', 'b', 'c'),
       LEAST('C', 'a', 'B'),
       LEAST('AAA', 'AaA', 'AAa'),
       LEAST('A', 'AA', 'AAA'),
       LEAST('11', '13', '12'),
       LEAST('11', '2', '12'),
       LEAST('01', '03', '02'),
       LEAST('01', '1', '02'),
       LEAST(null, 'b', 'c' ),
       LEAST('a', null, 'c'),
       LEAST('a', 'b', null ),
       LEAST('a', null, null),
       LEAST(null, 'b', null),
       LEAST(cast(null as string), null, null)
FROM src tablesample (1 rows);

SELECT LEAST(11, 13, 12),
       LEAST(1, 13, 2),
       LEAST(-11, -13, -12),
       LEAST(1, -13, 2),
       LEAST(null, 1, 2),
       LEAST(1, null, 2),
       LEAST(1, 2, null),
       LEAST(cast(null as int), null, null)
FROM src tablesample (1 rows);

SELECT LEAST(11.4, 13.5, 12.2),
       LEAST(1.0, 13.2, 2.0),
       LEAST(-11.4, -13.1, -12.2),
       LEAST(1.0, -13.3, 2.2),
       LEAST(null, 1.1, 2.2),
       LEAST(1.1, null, 2.2),
       LEAST(1.1, 2.2, null),
       LEAST(cast(null as double), null, null)
FROM src tablesample (1 rows);
