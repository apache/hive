DECLARE count INT DEFAULT 3;
WHILE 1=1 BEGIN
  PRINT 'Start of while block';
  PRINT count;
  SET count = count - 1;
  IF count = 0
    BREAK;
  PRINT 'End of while block';
END
PRINT 'End of script';