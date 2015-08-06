MAP OBJECT s TO default.src;
MAP OBJECT log TO src AT hive2conn;

DECLARE cnt INT DEFAULT 3; 
SELECT count(*) INTO cnt FROM s t1 WHERE 1=0;
PRINT cnt;
SET cnt = 5;
 
SELECT count(*) FROM log WHERE 1=0;