CREATE TABLE person (id INTEGER, country STRING);
SET hive.default.nulls.last=false;
EXPLAIN CBO SELECT country, count(1) FROM person GROUP BY country LIMIT 5;
SET hive.default.nulls.last=true;
EXPLAIN CBO SELECT country, count(1) FROM person GROUP BY country LIMIT 5;
