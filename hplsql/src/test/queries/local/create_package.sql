create or replace package pack1 as
  a int := 3;
  function f1 (p1 number, p2 number) return number; 
  procedure sp1(p1 number);
end;

create or replace package body pack1 as
  b int := 1;
  function f1 (p1 number, p2 number) return number
  is
    begin
      dbms_output.put_line('a: ' || a);
      dbms_output.put_line('b: ' || b);
      dbms_output.put_line('p1: ' || p1);
      dbms_output.put_line('p2: ' || p2);
      if 1=1 then
        dbms_output.put_line('f2: ' || f2());
      end if;
      dbms_output.put_line('pack1.f2: ' || pack1.f2());
      sp1(a);
      sp2(b);
      call sp3(a);
      return p1 + p2 + a + b;
    end; 

  function f2 return number
  is
    begin
      return 1;
    end;     
    
  procedure sp1(p1 number) 
  is
    begin
      dbms_output.put_line('a: ' || a);
      dbms_output.put_line('b: ' || b);
      dbms_output.put_line('p1: ' || p1);
    end;
    
  procedure sp3(p1 number)              -- private procedure
  is
    begin
      dbms_output.put_line('a: ' || a);
      dbms_output.put_line('b: ' || b);
      dbms_output.put_line('p1: ' || p1);
    end;
end;

create procedure sp2(p2 number) 
is
begin
  dbms_output.put_line('pack1.a: ' || pack1.a);
  dbms_output.put_line('p2: ' || p2);
end;

dbms_output.put_line('pack1.a: ' || pack1.a);
dbms_output.put_line('pack1.f1: ' || pack1.f1(3,5));
pack1.sp1(1);
call pack1.sp1(1);

