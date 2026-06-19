BEGIN
  PRINT 'Correct';
  WHILE 1=1 THEN
    FETCH cur INTO v;
    PRINT 'Incorrect - unreachable code, unknown cursor name, exception must be raised';
  END WHILE;
EXCEPTION WHEN OTHERS THEN
  PRINT 'Correct';
  PRINT 'Correct';
  PRINT 'Correct - Exception raised';   
  WHEN NO_DATA_FOUND THEN
  PRINT 'Correct';
END 

