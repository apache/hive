create procedure p(n number)
begin
  declare v int = n + 1;
  print 'v=' || v;
end;

declare an1 number = 1.3;
declare an2 number = an1 + 1.2;
print 'an2=' || an2;

declare sn1 number = 1.3;
declare sn2 number = sn1 - 1.2;
print 'sn2=' || sn2;

declare mn1 number = 4.2;
declare mn2 number = mn1 * 2.1;
print 'mn2=' || mn2;

declare dn1 number = 4.2;
declare dn2 number = dn1 / 2.4;
print 'dn2=' || dn2;

p(42);
p('12');
p('not a number');

