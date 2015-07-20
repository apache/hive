DECLARE var1 INT DEFAULT 3;
PRINT DECODE (var1, 1, 'A', 2, 'B', 3, 'C');
PRINT DECODE (var1, 1, 'A', 2, 'B', 'C');

SET var1 := 1;
PRINT DECODE (var1, 1, 'A', 2, 'B', 3, 'C');

SET var1 := NULL;
PRINT DECODE (var1, 1, 'A', 2, 'B', NULL, 'C');
PRINT DECODE (var1, 1, 'A', 2, 'B', 'C');
