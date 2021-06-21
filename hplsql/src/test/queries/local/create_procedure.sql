CREATE PROCEDURE set_message(IN name STRING, OUT result STRING)
BEGIN
  SET result = 'Hello, ' || name || '!';
END;

DECLARE str STRING;
CALL set_message('world', str);
PRINT str;

