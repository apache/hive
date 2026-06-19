CREATE PROCEDURE load_tab(IN name STRING, OUT result STRING)
BEGIN
  DROP TABLE IF EXISTS test_tab;

  CREATE TABLE IF NOT EXISTS test_tab
  STORED AS ORC
  AS
  SELECT *
  FROM src;

  SET result = name;

END;

DECLARE str STRING;
CALL load_tab('world', str);
PRINT str; 