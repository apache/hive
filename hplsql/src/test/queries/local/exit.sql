DECLARE count INT DEFAULT 3;

WHILE 1=1 LOOP
  PRINT 'Start of while block';
  PRINT count;
  count := count - 1;
  EXIT WHEN count = 0;
  PRINT 'End of while block';
END LOOP;

count := 3;

<<lbl>>
WHILE 1=1 LOOP
  PRINT 'Start of outer while block';
  
  WHILE 1=1 LOOP
    PRINT 'Start of 1st inner while block';
    EXIT;
    PRINT 'End of 1st inner while block (NEVER SHOWN)';
  END LOOP;
  
  <<lbl2>>
  WHILE 1=1 LOOP
    PRINT 'Start of 2nd inner while block';
    EXIT lbl;
    PRINT 'End of 2nd inner while block (NEVER SHOWN)';
  END LOOP;
  PRINT 'End of outer while block (NEVER SHOWN)';
END LOOP;
PRINT 'End of script';