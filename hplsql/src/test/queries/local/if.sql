DECLARE state VARCHAR;
DECLARE count INT;

SET state = 'CA';
SET count = 1;

IF count = 1 THEN
  PRINT 'True block - Correct';
END IF;

IF state = 'CA' THEN
  PRINT 'True block - Correct';
ELSE
  PRINT 'False block - Incorrect';
END IF;

IF state = 'MA' THEN
  PRINT 'True block - Incorrect';
ELSE
  PRINT 'False block - Correct';
END IF;

IF count = 4 THEN
  PRINT 'True block - Incorrect';  
ELSIF count = 3 THEN
  PRINT 'True block - Incorrect';  
ELSIF count = 2 THEN
  PRINT 'True block - Incorrect';  
ELSE
  PRINT 'False block - Correct'; 
END IF;

IF count = 3 THEN
  PRINT 'True block - Incorrect';  
ELSIF count = 2 THEN
  PRINT 'True block - Incorrect';  
ELSIF count = 1 THEN
  PRINT 'True block - Correct';  
ELSE
  PRINT 'False block - Incorrect'; 
END IF;

PRINT 'IS NOT NULL AND BETWEEN';
IF 1 IS NOT NULL AND 1 BETWEEN 0 AND 100 THEN
  PRINT 'True block - Correct';  
ELSE
  PRINT 'False block - Incorrect'; 
END IF;

PRINT 'Transact-SQL - Single statement';

IF state = 'CA'
  PRINT 'True block - Correct';  
ELSE 
  PRINT 'False block - Incorrect'; 

PRINT 'Transact-SQL - BEGIN-END block'; 
  
IF state = 'CA'
BEGIN
  PRINT 'True block - Correct'; 
  PRINT 'True block - Correct'; 
END
ELSE 
BEGIN
  PRINT 'False block - Incorrect'; 
  PRINT 'False block - Incorrect'; 
END  