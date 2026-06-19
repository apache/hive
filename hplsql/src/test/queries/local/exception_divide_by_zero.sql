DECLARE
  booknum int;
  total int;
  percent int;
BEGIN
  SET booknum = 10;
  SET total = 0;
  SET percent = booknum / total;
EXCEPTION WHEN OTHERS THEN
  DBMS_OUTPUT.PUT_LINE('Correct');
END;