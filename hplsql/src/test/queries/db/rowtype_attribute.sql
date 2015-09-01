DECLARE
  v1 default.src%ROWTYPE;
  v2 src %ROWTYPE;
  v3 src % ROWTYPE;  
  CURSOR c1 IS SELECT 'A' AS key, 'B' AS value FROM src LIMIT 1;
BEGIN
  SELECT 'A' AS key, 'B' AS value INTO v1 FROM src LIMIT 1;
  PRINT v1.key || v1.value;
  
  OPEN c1;
  FETCH c1 INTO v2;
  PRINT v2.key || v2.value;
  CLOSE c1;
  
  FOR rec IN (SELECT 'A' AS key, 'B' AS value FROM src LIMIT 1)
  LOOP
    PRINT rec.key || rec.value;
  END LOOP; 
  
  EXECUTE IMMEDIATE 'SELECT ''A'' AS key, ''B'' AS value FROM src LIMIT 1' INTO v3;
  PRINT v3.key || v3.value; 
END