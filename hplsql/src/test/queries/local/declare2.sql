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
