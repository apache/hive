DECLARE cnt INT := 0;
PRINT 'Correct';
DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
  PRINT 'Correct - Exception raised';    
WHILE cnt < 10 THEN
FETCH cur INTO v;
PRINT cnt;
PRINT 'Correct - exception handled';
SET cnt = cnt + 1;
END WHILE;
