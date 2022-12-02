CREATE TABLE customer(orders array<string>);


set hive.optimize.ppd=false;
set hive.explain.user=false;
SELECT *
FROM customer
lateral view explode(orders) v as c1
lateral view explode(orders) v as c2
lateral view explode(orders) v as c3
lateral view explode(orders) v as c4
;
