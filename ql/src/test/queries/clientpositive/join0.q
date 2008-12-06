EXPLAIN
SELECT * FROM
  (SELECT * FROM src WHERE src.key < 10) src1 
    JOIN 
  (SELECT * FROM src WHERE src.key < 10) src2;

SELECT * FROM
  (SELECT * FROM src WHERE src.key < 10) src1 
    JOIN 
  (SELECT * FROM src WHERE src.key < 10) src2;

