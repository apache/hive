create procedure p(a int, b int)
begin
  print 'a=' || a;
  print 'b=' || b;
end;

call p(1,2);
call p(1);
