include 'src/test/queries/local/var_scope_include.sql'

declare i int = 3;

proc p1  
begin
  print 'i: ' || i;
  print 'j: ' || j;
  print 'k: ' || k;
end;


proc p2  
begin
  declare j int = 5;
  print 'i: ' || i;
  print 'j: ' || j;
  print 'k: ' || k;
  p1();
end;

p2();
print 'i: ' || i;
print 'j: ' || j;
print 'k: ' || k;



