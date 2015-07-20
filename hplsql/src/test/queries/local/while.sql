DECLARE count INT DEFAULT 7;

WHILE count <> 0 LOOP
  PRINT count;
  count := count - 1;
END LOOP;

SET count = 7;

WHILE count <> 0 DO
  PRINT count;
  SET count = count - 1;
END WHILE;

SET count = 7;

WHILE count <> 0 BEGIN
  PRINT count;
  SET count = count - 1;
END