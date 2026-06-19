CREATE PROCEDURE set_message(IN name STRING, OUT result STRING)
BEGIN
  DECLARE str STRING DEFAULT 'Hello, ' || name || '!';
  Work: begin
    declare continue handler for sqlexception begin
      set result = null;
      print 'error';
    end;
    set result = str;
  end;
END;
 
DECLARE str STRING;
CALL set_message('world', str);
PRINT str;

