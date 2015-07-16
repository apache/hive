CREATE FUNCTION hello2(text STRING)
  RETURNS STRING
BEGIN
  PRINT 'Start';
  RETURN 'Hello, ' || text || '!';
  PRINT 'Must not be printed';
END;
 
-- Call the function
PRINT hello2('wor' || 'ld');
PRINT 'End of script';