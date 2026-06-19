PRINT 'a' || 'b';
PRINT 'a' || 1 || 'b';
PRINT 1 || 'a' || 'b';
PRINT 'a' || null || 'b';
PRINT null || 'a' || 'b';
PRINT null || null;

DECLARE c INT;

PRINT 'Integer increment'; 
c := 3;
c := c + 1;
PRINT c;

PRINT 'Integer decrement'; 
c := 3;
c := c - 1;
PRINT c;

PRINT NVL(null - 3, 'Correct');
PRINT NVL(null + 3, 'Correct');