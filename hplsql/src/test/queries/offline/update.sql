UPDATE tab T SET (c1) = TRIM(c1) WHERE T.c2 = 'A';

UPDATE tab T 
  SET c1 = TRIM(c1) 
  WHERE T.c2 = 'A';
  
UPDATE tab SET c1 = '0011' WHERE c1 = '0021';

UPDATE tab T SET c1 = TRIM(c1), c3 = TRIM(c3) WHERE T.col2 = 'A';

UPDATE tab T 
  SET (c1, c3) = (TRIM(c1), TRIM(c3)) 
  WHERE T.col2 = 'A';

UPDATE tab T
       SET (c1, c2, c3, c4) =
           (SELECT c1,
                   c2,
                   TRIM(c3),
                   c4
              FROM tab2 C
             WHERE C.c1 = T.c1)
     WHERE T.c2 = 'A';
     
UPDATE tab T
       SET (c1) =
           (SELECT c1 FROM tab2 C WHERE C.c1 = T.c1)
     WHERE T.c2 = 'a';
       
UPDATE tab T
       SET c1 =
           (SELECT c1 FROM tab2 C WHERE C.c1 = T.c1)
     WHERE T.c2 = 'a';
