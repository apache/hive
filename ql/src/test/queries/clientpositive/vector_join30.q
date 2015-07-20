set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.mapjoin.native.enabled=true;
set hive.fetch.task.conversion=none;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;

-- SORT_QUERY_RESULTS

CREATE TABLE orcsrc STORED AS ORC AS SELECT * FROM src;

explain
FROM 
(SELECT orcsrc.* FROM orcsrc sort by key) x
JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Y
ON (x.key = Y.key)
select sum(hash(Y.key,Y.value));

FROM 
(SELECT orcsrc.* FROM orcsrc sort by key) x
JOIN 
(SELECT orcsrc.* FROM orcsrc sort by value) Y
ON (x.key = Y.key)
select sum(hash(Y.key,Y.value));

explain
FROM 
(SELECT orcsrc.* FROM orcsrc sort by key) x
LEFT OUTER JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Y
ON (x.key = Y.key)
select sum(hash(Y.key,Y.value));

FROM 
(SELECT orcsrc.* FROM orcsrc sort by key) x
LEFT OUTER JOIN 
(SELECT orcsrc.* FROM orcsrc sort by value) Y
ON (x.key = Y.key)
select sum(hash(Y.key,Y.value));

explain
FROM 
(SELECT orcsrc.* FROM orcsrc sort by key) x
RIGHT OUTER JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Y
ON (x.key = Y.key)
select sum(hash(Y.key,Y.value));

FROM 
(SELECT orcsrc.* FROM orcsrc sort by key) x
RIGHT OUTER JOIN 
(SELECT orcsrc.* FROM orcsrc sort by value) Y
ON (x.key = Y.key)
select sum(hash(Y.key,Y.value));

explain
FROM 
(SELECT orcsrc.* FROM orcsrc sort by key) x
JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Y
ON (x.key = Y.key)
JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));

FROM
(SELECT orcsrc.* FROM orcsrc sort by key) x
JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Y
ON (x.key = Y.key)
JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));

explain
FROM 
(SELECT orcsrc.* FROM orcsrc sort by key) x
JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Y
ON (x.key = Y.key)
LEFT OUTER JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));

FROM
(SELECT orcsrc.* FROM orcsrc sort by key) x
JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Y
ON (x.key = Y.key)
LEFT OUTER JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));

explain
FROM 
(SELECT orcsrc.* FROM orcsrc sort by key) x
LEFT OUTER JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Y
ON (x.key = Y.key)
LEFT OUTER JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));

FROM
(SELECT orcsrc.* FROM orcsrc sort by key) x
LEFT OUTER JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Y
ON (x.key = Y.key)
LEFT OUTER JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));

explain
FROM 
(SELECT orcsrc.* FROM orcsrc sort by key) x
LEFT OUTER JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Y
ON (x.key = Y.key)
RIGHT OUTER JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));

FROM
(SELECT orcsrc.* FROM orcsrc sort by key) x
LEFT OUTER JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Y
ON (x.key = Y.key)
RIGHT OUTER JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));

explain
FROM 
(SELECT orcsrc.* FROM orcsrc sort by key) x
RIGHT OUTER JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Y
ON (x.key = Y.key)
RIGHT OUTER JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));

FROM
(SELECT orcsrc.* FROM orcsrc sort by key) x
RIGHT OUTER JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Y
ON (x.key = Y.key)
RIGHT OUTER JOIN
(SELECT orcsrc.* FROM orcsrc sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));
