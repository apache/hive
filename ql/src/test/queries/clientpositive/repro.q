CREATE TABLE customer(orders array<string>);

EXPLAIN SELECT *
FROM customer
lateral view explode(orders) v as c1
lateral view explode(orders) v as c2
;

EXPLAIN FORMATTED SELECT *
FROM customer
lateral view explode(orders) v as c1
lateral view explode(orders) v as c2
;

EXPLAIN AST SELECT *
FROM customer
lateral view explode(orders) v as c1
lateral view explode(orders) v as c2
;

set hive.explain.user=true;
EXPLAIN SELECT *
FROM customer
lateral view explode(orders) v as c1
lateral view explode(orders) v as c2
;

EXPLAIN FORMATTED SELECT *
FROM customer
lateral view explode(orders) v as c1
lateral view explode(orders) v as c2
;