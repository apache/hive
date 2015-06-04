set hive.support.sql11.reserved.keywords=false;

DESCRIBE FUNCTION collect_set;
DESCRIBE FUNCTION EXTENDED collect_set;

DESCRIBE FUNCTION collect_list;
DESCRIBE FUNCTION EXTENDED collect_list;


-- initialize tables

CREATE TABLE customers (id int, name varchar(10), age int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH "../../data/files/customers.txt" INTO TABLE customers;

CREATE TABLE orders (id int, cid int, date date, amount double)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH "../../data/files/orders.txt" INTO TABLE orders;

CREATE TABLE nested_orders (id int, cid int, date date, sub map<string,double>)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '$'
MAP KEYS TERMINATED BY ':';

LOAD DATA LOCAL INPATH "../../data/files/nested_orders.txt" INTO TABLE nested_orders;

-- 1. test struct

-- 1.1 when field is primitive

SELECT c.id, sort_array(collect_set(named_struct("name", c.name, "date", o.date, "amount", o.amount)))
FROM customers c
INNER JOIN orders o
ON (c.id = o.cid) GROUP BY c.id;

SELECT c.id, sort_array(collect_list(named_struct("name", c.name, "date", o.date, "amount", o.amount)))
FROM customers c
INNER JOIN orders o
ON (c.id = o.cid) GROUP BY c.id;

-- cast decimal

SELECT c.id, sort_array(collect_set(named_struct("name", c.name, "date", o.date, "amount", cast(o.amount as decimal(10,1)))))
FROM customers c
INNER JOIN orders o
ON (c.id = o.cid) GROUP BY c.id;

SELECT c.id, sort_array(collect_list(named_struct("name", c.name, "date", o.date, "amount", cast(o.amount as decimal(10,1)))))
FROM customers c
INNER JOIN orders o
ON (c.id = o.cid) GROUP BY c.id;


SELECT c.id, sort_array(collect_set(struct(c.name, o.date, o.amount)))
FROM customers c
INNER JOIN orders o
ON (c.id = o.cid) GROUP BY c.id;

SELECT c.id, sort_array(collect_list(struct(c.name, o.date, o.amount)))
FROM customers c
INNER JOIN orders o
ON (c.id = o.cid) GROUP BY c.id;


-- 1.2 when field is map

SELECT c.id, sort_array(collect_set(named_struct("name", c.name, "date", o.date, "sub", o.sub)))
FROM customers c
INNER JOIN nested_orders o
ON (c.id = o.cid) GROUP BY c.id;

SELECT c.id, sort_array(collect_list(named_struct("name", c.name, "date", o.date, "sub", o.sub)))
FROM customers c
INNER JOIN nested_orders o
ON (c.id = o.cid) GROUP BY c.id;

SELECT c.id, sort_array(collect_set(struct(c.name, o.date, o.sub)))
FROM customers c
INNER JOIN nested_orders o
ON (c.id = o.cid) GROUP BY c.id;

SELECT c.id, sort_array(collect_list(struct(c.name, o.date, o.sub)))
FROM customers c
INNER JOIN nested_orders o
ON (c.id = o.cid) GROUP BY c.id;


-- 1.3 when field is list

SELECT c.id, sort_array(collect_set(named_struct("name", c.name, "date", o.date, "sub", map_values(o.sub))))
FROM customers c
INNER JOIN nested_orders o
ON (c.id = o.cid) GROUP BY c.id;

SELECT c.id, sort_array(collect_list(named_struct("name", c.name, "date", o.date, "sub", map_values(o.sub))))
FROM customers c
INNER JOIN nested_orders o
ON (c.id = o.cid) GROUP BY c.id;

SELECT c.id, sort_array(collect_set(struct(c.name, o.date, map_values(o.sub))))
FROM customers c
INNER JOIN nested_orders o
ON (c.id = o.cid) GROUP BY c.id;

SELECT c.id, sort_array(collect_list(struct(c.name, o.date, map_values(o.sub))))
FROM customers c
INNER JOIN nested_orders o
ON (c.id = o.cid) GROUP BY c.id;


-- 2. test array

-- 2.1 when field is primitive

SELECT c.id, sort_array(collect_set(array(o.amount)))
FROM customers c
INNER JOIN orders o
ON (c.id = o.cid) GROUP BY c.id;

SELECT c.id, sort_array(collect_list(array(o.amount)))
FROM customers c
INNER JOIN orders o
ON (c.id = o.cid) GROUP BY c.id;

-- cast decimal

SELECT c.id, sort_array(collect_set(array(cast(o.amount as decimal(10,1)))))
FROM customers c
INNER JOIN orders o
ON (c.id = o.cid) GROUP BY c.id;

SELECT c.id, sort_array(collect_list(array(cast(o.amount as decimal(10,1)))))
FROM customers c
INNER JOIN orders o
ON (c.id = o.cid) GROUP BY c.id;

-- 2.2 when field is struct

SELECT c.id, sort_array(collect_set(array(o.sub)))
FROM customers c
INNER JOIN nested_orders o
ON (c.id = o.cid) GROUP BY c.id;

SELECT c.id, sort_array(collect_list(array(o.sub)))
FROM customers c
INNER JOIN nested_orders o
ON (c.id = o.cid) GROUP BY c.id;

-- 2.3 when field is list

SELECT c.id, sort_array(collect_set(array(map_values(o.sub))))
FROM customers c
INNER JOIN nested_orders o
ON (c.id = o.cid) GROUP BY c.id;

SELECT c.id, sort_array(collect_list(array(map_values(o.sub))))
FROM customers c
INNER JOIN nested_orders o
ON (c.id = o.cid) GROUP BY c.id;


-- 3. test map

-- 3.1 when field is primitive

SELECT c.id, sort_array(collect_set(map("amount", o.amount)))
FROM customers c
INNER JOIN orders o
ON (c.id = o.cid) GROUP BY c.id;

SELECT c.id, sort_array(collect_list(map("amount", o.amount)))
FROM customers c
INNER JOIN orders o
ON (c.id = o.cid) GROUP BY c.id;

-- cast decimal

SELECT c.id, sort_array(collect_set(map("amount", cast(o.amount as decimal(10,1)))))
FROM customers c
INNER JOIN orders o
ON (c.id = o.cid) GROUP BY c.id;

SELECT c.id, sort_array(collect_list(map("amount", cast(o.amount as decimal(10,1)))))
FROM customers c
INNER JOIN orders o
ON (c.id = o.cid) GROUP BY c.id;

-- 3.2 when field is struct

SELECT c.id, sort_array(collect_set(map("sub", o.sub)))
FROM customers c
INNER JOIN nested_orders o
ON (c.id = o.cid) GROUP BY c.id;

SELECT c.id, sort_array(collect_list(map("sub", o.sub)))
FROM customers c
INNER JOIN nested_orders o
ON (c.id = o.cid) GROUP BY c.id;

-- 3.3 when field is list

SELECT c.id, sort_array(collect_set(map("sub", map_values(o.sub))))
FROM customers c
INNER JOIN nested_orders o
ON (c.id = o.cid) GROUP BY c.id;

SELECT c.id, sort_array(collect_list(map("sub", map_values(o.sub))))
FROM customers c
INNER JOIN nested_orders o
ON (c.id = o.cid) GROUP BY c.id;


-- clean up

DROP TABLE customer;
DROP TABLE orders;
DROP TABLE nested_orders
