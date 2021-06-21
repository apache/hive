DECLARE c INT = 0;
LOOP
  c = c + 1;
  EXIT WHEN c = 5;
END LOOP;

PRINT 'EXITED: ' || c