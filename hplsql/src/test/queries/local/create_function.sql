CREATE FUNCTION hello()
 RETURNS STRING
BEGIN
 PRINT 'Start';
 RETURN 'Hello, world';
 PRINT 'Must not be printed';
END;
 
-- Call the function
PRINT hello() || '!';
PRINT 'End of script';