IF INSTR('abc', 'b') = 2 THEN
  PRINT 'Correct';
ELSE
  PRINT 'Failed';
END IF;

IF INSTR('abcabc', 'b', 3) = 5 THEN
  PRINT 'Correct';
ELSE
  PRINT 'Failed';
END IF;

IF INSTR('abcabcabc', 'b', 3, 2) = 8 THEN
  PRINT 'Correct';
ELSE
  PRINT 'Failed';
END IF; 

IF INSTR('abcabcabc', 'b', -3) = 5 THEN
  PRINT 'Correct';
ELSE
  PRINT 'Failed';
END IF; 

IF INSTR('abcabcabc', 'b', -3, 2) = 2 THEN
  PRINT 'Correct';
ELSE
  PRINT 'Failed';
END IF;

DECLARE c STRING;

IF INSTR(c, 'b') IS NULL THEN
  PRINT 'Correct';
ELSE
  PRINT 'Failed';
END IF;

IF INSTR(NULL, 'b') IS NULL THEN
  PRINT 'Correct';
ELSE
  PRINT 'Failed';
END IF;

IF INSTR('', 'b') = 0 THEN
  PRINT 'Correct';
ELSE
  PRINT 'Failed';
END IF;