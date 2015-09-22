DECLARE v_bint BIGINT;
DECLARE v_int INT;
DECLARE v_sint SMALLINT;
DECLARE v_tint TINYINT;
DECLARE v_dec DECIMAL(18,2);
DECLARE v_dec0 DECIMAL(18,0);
DECLARE v_str STRING;

SELECT TOP 1 
  CAST(1 AS BIGINT),
  CAST(1 AS INT), 
  CAST(1 AS SMALLINT), 
  CAST(1 AS TINYINT), 
  CAST(1.1 AS DECIMAL(18,2)),
  CAST(1.1 AS DECIMAL(18,0))   
INTO 
  v_bint,
  v_int,
  v_sint,
  v_tint,
  v_dec,
  v_dec0  
FROM src;
        
PRINT 'BIGINT: ' || v_bint;
PRINT 'INT: ' || v_int;
PRINT 'SMALLINT: ' || v_sint;
PRINT 'TINYINT: ' || v_tint;
PRINT 'DECIMAL: ' || v_dec;
PRINT 'DECIMAL0: ' || v_dec0;

select 'a' into v_str from src limit 1;
print 'string: ' || v_str;