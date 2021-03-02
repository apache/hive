
CREATE TABLE alltypestiny (id int, int_col int);

INSERT INTO alltypestiny values (3, 8);
INSERT INTO alltypestiny values (3, 9);
INSERT INTO alltypestiny values (4, 9);

SELECT id
FROM alltypestiny t1
WHERE EXISTS
  (SELECT 1
   FROM alltypestiny t2
   WHERE t1.id = t2.id
   GROUP BY t2.id
   HAVING count(1) = 1);
