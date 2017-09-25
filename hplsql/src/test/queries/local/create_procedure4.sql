CREATE PROCEDURE test1()
AS
  ret string DEFAULT 'Initial value';
BEGIN
  print(ret);
  ret := 'VALUE IS SET';
  print(ret);
END;

test1();
