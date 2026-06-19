declare b1 bool = true;
declare b2 boolean = false;

set b1 = false;
set b2 = true;

if b2 then
  print 'ok';
else
  print 'failed';
end if;  

print b1;
print b2;