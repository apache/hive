CREATE FUNCTION get_count()
 RETURNS STRING
BEGIN
 DECLARE cnt INT = 0;
 SELECT COUNT(*) INTO cnt FROM src
 WHERE value RLIKE '^[+-]?[0-9]*[.]?[0-9]*$';
 RETURN cnt;
END;

-- Call the function
PRINT get_count();