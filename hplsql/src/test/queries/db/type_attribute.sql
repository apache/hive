DECLARE
  v1 default.src.key%TYPE;
  v2 src.Key %TYPE;
  v3 src.key3 % TYPE;
BEGIN
  SELECT 'A', 'B', 1 INTO v1, v2, v3 FROM src LIMIT 1;
  PRINT v1 || v2 || v3;
END