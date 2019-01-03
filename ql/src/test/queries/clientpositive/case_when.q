CREATE TABLE IF NOT EXISTS `Calcs`(
   `bool0` boolean,
   `bool1` boolean,
   `bool2` boolean)
STORED AS ORC;


INSERT INTO Calcs VALUES
( NULL   , true   , false ),
( true   , true   , false ),
( false  , true   , true  ),
( NULL   , false  , true  ),
( NULL   , false  , true  ),
( true   , false  , true  ),
( false  , NULL   , false ),
( NULL   , NULL   , false ),
( true   , true   , false ),
( false  , true   , false ),
( NULL   , true   , false ),
( true   , false  , false ),
( false  , false  , true  ),
( NULL   , false  , true  ),
( true   , NULL   , false ),
( false  , NULL   , true  ),
( NULL   , NULL   , false );


SELECT
   bool0,
   bool1,
   bool2,
   ((CASE
      WHEN bool0 THEN (CASE WHEN bool1 THEN 1 WHEN NOT bool1 THEN 0 ELSE NULL END)
      ELSE (CASE WHEN bool2 THEN 1 WHEN NOT bool2 THEN 0 ELSE NULL END) END) = 1) AS res
FROM Calcs
ORDER BY bool0, bool1, bool2;

DROP TABLE IF EXISTS `Calcs`;