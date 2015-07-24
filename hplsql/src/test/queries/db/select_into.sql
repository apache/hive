DECLARE v_int INT;
DECLARE v_dec DECIMAL(18,2);
DECLARE v_dec0 DECIMAL(18,0);

SELECT TOP 1 
  CAST(1 AS INT), 
  CAST(1.1 AS DECIMAL(18,2)),
  CAST(1.1 AS DECIMAL(18,0))   
INTO 
  v_int,
  v_dec,
  v_dec0  
FROM src ;
        
PRINT 'INT: ' || v_int;
PRINT 'DECIMAL: ' || v_dec;
PRINT 'DECIMAL0: ' || v_dec0;