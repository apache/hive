DECLARE count INT DEFAULT 3;
lbl:
WHILE 1=1 DO
  PRINT 'Start of while block';
  PRINT count;
  SET count = count - 1;
  IF count = 0 THEN
    LEAVE lbl;
  END IF;
  PRINT 'End of while block';
END WHILE;

SET count = 3;

lbl3:
WHILE 1=1 DO
  PRINT 'Start of outer while block';
  
  lbl1:
  WHILE 1=1 DO
    PRINT 'Start of 1st inner while block';
    LEAVE lbl1;
    PRINT 'End of 1st inner while block (NEVER SHOWN)';
  END WHILE;
  
  lbl2:
  WHILE 1=1 DO
    PRINT 'Start of 2nd inner while block';
    LEAVE lbl3;
    PRINT 'End of 2nd inner while block (NEVER SHOWN)';
  END WHILE;
  PRINT 'End of outer while block (NEVER SHOWN)';
END WHILE;