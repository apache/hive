EXPLAIN CREATE TEMPORARY TABLE foo AS SELECT * FROM src WHERE key % 2 = 0;
CREATE TEMPORARY TABLE foo AS SELECT * FROM src WHERE key % 2 = 0;

EXPLAIN CREATE TEMPORARY TABLE bar AS SELECT * FROM src WHERE key % 2 = 1;
CREATE TEMPORARY TABLE bar AS SELECT * FROM src WHERE key % 2 = 1;

DESCRIBE foo;
DESCRIBE bar;

explain select * from foo limit 10;
select * from foo limit 10;

explain select * from (select * from foo union all select * from bar) u order by key limit 10;
select * from (select * from foo union all select * from bar) u order by key limit 10;

CREATE TEMPORARY TABLE baz LIKE foo;

INSERT OVERWRITE TABLE baz SELECT * from foo;

CREATE TEMPORARY TABLE bay (key string, value string) STORED AS orc;
select * from bay;

INSERT OVERWRITE TABLE bay SELECT * FROM src ORDER BY key;

select * from bay limit 10;

SHOW TABLES;

CREATE DATABASE two;

USE two;

SHOW TABLES;

CREATE TEMPORARY TABLE foo AS SELECT * FROM default.foo;

SHOW TABLES;

use default;

DROP DATABASE two CASCADE;

DROP TABLE bay;
