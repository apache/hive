DECLARE
  str VARCHAR(200) DEFAULT 'Hello, world!';
BEGIN
  DBMS_OUTPUT.PUT_LINE('Hello, world!');
  DBMS_OUTPUT.PUT_LINE(str);
  DBMS_OUTPUT.PUT_LINE(trim('a''a'));
  DBMS_OUTPUT.PUT_LINE(trim('''a'));
  DBMS_OUTPUT.PUT_LINE(trim('a'''));
END;
