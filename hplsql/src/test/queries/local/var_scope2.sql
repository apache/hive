declare i int = 3;

function f1(p1 int) return int
begin
  declare j int = 7;
  print 'p1: ' || p1;
  return f2(j);
end

function f2(p1 int) return int
begin
  print 'p1: ' || p1;
  return p1;
end

proc sp1(p1 int)
begin
  declare j int = 7;
  print 'p1: ' || p1;
  call sp2(j);
end

proc sp2(p1 int)
begin
  print 'p1: ' || p1;
end


f1(i);
call sp1(i);
