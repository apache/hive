CREATE TABLE person (id INT, fname STRING);
set hive.explain.formatted.indent=true;
EXPLAIN FORMATTED SELECT fname FROM person WHERE id > 100;
set hive.explain.formatted.indent=false;
EXPLAIN FORMATTED SELECT fname FROM person WHERE id > 100;