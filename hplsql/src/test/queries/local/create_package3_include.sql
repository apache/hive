create or replace package a as
procedure test(); 
end; 

create or replace package body a as 
procedure test() 
is 
begin 
print "test ok"; 
end; 
end;