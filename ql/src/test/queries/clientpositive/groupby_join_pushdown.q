set hive.mapred.mode=nonstrict;
set hive.transpose.aggr.join=true;
EXPLAIN
SELECT f.key, g.key, count(g.key)
FROM src f JOIN src g ON(f.key = g.key)
GROUP BY f.key, g.key;

EXPLAIN
SELECT f.key, g.key
FROM src f JOIN src g ON(f.key = g.key)
GROUP BY f.key, g.key;

EXPLAIN
SELECT DISTINCT f.value, g.value
FROM src f JOIN src g ON(f.value = g.value);

EXPLAIN
SELECT f.key, g.key, COUNT(*)
FROM src f JOIN src g ON(f.key = g.key)
GROUP BY f.key, g.key;

EXPLAIN
SELECT  f.ctinyint, g.ctinyint, SUM(f.cbigint)              
FROM alltypesorc f JOIN alltypesorc g ON(f.cint = g.cint)
GROUP BY f.ctinyint, g.ctinyint ;

EXPLAIN
SELECT  f.cbigint, g.cbigint, MAX(f.cint)              
FROM alltypesorc f JOIN alltypesorc g ON(f.cbigint = g.cbigint)
GROUP BY f.cbigint, g.cbigint ;

explain
SELECT  f.ctinyint, g.ctinyint, MIN(f.ctinyint)              
FROM alltypesorc f JOIN alltypesorc g ON(f.ctinyint = g.ctinyint)
GROUP BY f.ctinyint, g.ctinyint;

explain
SELECT   MIN(f.cint)     
FROM alltypesorc f JOIN alltypesorc g ON(f.ctinyint = g.ctinyint)
GROUP BY f.ctinyint, g.ctinyint;

explain
SELECT   count(f.ctinyint)              
FROM alltypesorc f JOIN alltypesorc g ON(f.ctinyint = g.ctinyint)
GROUP BY f.ctinyint, g.ctinyint;

explain
SELECT   count(f.cint), f.ctinyint              
FROM alltypesorc f JOIN alltypesorc g ON(f.ctinyint = g.ctinyint)
GROUP BY f.ctinyint, g.ctinyint;

explain
SELECT   sum(f.cint), f.ctinyint            
FROM alltypesorc f JOIN alltypesorc g ON(f.ctinyint = g.ctinyint)
GROUP BY f.ctinyint, g.ctinyint;

