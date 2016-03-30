create or replace procedure proc1(out v_msg string)
declare
  num1 int=0;
  num2 int=10;
  num3 int=30;
begin
  print num1;
  print num2;
  print num3;
  set v_msg = 'Completed';
end;

create or replace procedure proc2(out v_msg string)
is
  num1 int=0;
  num2 int=10;
  num3 int=30;
begin
  print num1;
  print num2;
  print num3;
  set v_msg = 'Completed';
end;

declare str string;
call proc1(str);
print str;
call proc2(str);
print str;
