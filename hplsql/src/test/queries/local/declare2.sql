declare 
  code char(10) not null := 'a';
  status constant int := 1;
begin
  print code;
  print status;
end;

declare 
  code char(10) not null := 'a';
begin
  null;
end;

declare
  num1 int := 1;
  num2 int := 2;
begin
  print num1;
  print -num1;
  print -num1*2+num2;
end;