create procedure sp1
begin
  print 'a';
end;

create procedure sp2()
begin
  print 'b';
end;

call sp1;
call sp1();
sp1;
sp1();

call sp2;
call sp2();
sp2;
sp2();