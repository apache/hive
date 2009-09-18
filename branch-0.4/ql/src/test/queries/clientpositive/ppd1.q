set hive.optimize.ppd=true;

EXPLAIN
SELECT src.key as c3 from src where src.key > '2';

SELECT src.key as c3 from src where src.key > '2';

