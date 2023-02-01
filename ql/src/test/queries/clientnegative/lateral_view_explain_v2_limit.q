CREATE TABLE customer(orders array<string>);

EXPLAIN EXTENDED SELECT *
FROM customer
lateral view explode(orders) v as c1
lateral view explode(orders) v as c2
lateral view explode(orders) v as c3
lateral view explode(orders) v as c4
lateral view explode(orders) v as c5
lateral view explode(orders) v as c6
lateral view explode(orders) v as c7
lateral view explode(orders) v as c8
lateral view explode(orders) v as c9
;
