CREATE TABLE t (a1 INT, a2 INT, a1a int, a2a int);

insert into t values (1,2,10,20);

SET hive.support.quoted.identifiers=none;

SELECT `(a1)?+.+` FROM t
UNION
SELECT `(a2)?+.+` FROM t;

SELECT `(a1)?+.+` FROM t
UNION DISTINCT
SELECT `(a2)?+.+` FROM t;

SELECT `(a1)?+.+` FROM t
UNION ALL
SELECT `(a2)?+.+` FROM t;
