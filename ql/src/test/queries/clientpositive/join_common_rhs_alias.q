CREATE TABLE tab (c1 STRING, c2 STRING, c3 STRING);

INSERT INTO tab VALUES("a", "a", "aa"), ("b", "b", "ba"), ("c", "c" , "a");

SELECT t1.* FROM tab t1 LEFT OUTER JOIN tab t2
ON t1.c1 == t2.c1
AND CONCAT ( t1.c2 , 'a') = CONCAT ( t2.c2 , t2.c3 )
WHERE t2.c1 IS NULL;
