declare a BIGINT = 3;
declare b DECIMAL = 4;
declare c DOUBLE = 5;
declare s STRING = 'abc';

-- add
a + a;
a + b;
a + c;

b + b;
b + a;
b + c;

c + c;
c + a;
c + b;
s + s;

-- sub
a - a;
a - b;
a - c;

b - b;
b - a;
b - c;

c - c;
c - a;
c - b;

-- mul
a * a;
a * b;
a * c;

b * b;
b * a;
b * c;

c * c;
c * a;
c * b;

-- div
a / a;
a / b;
a / c;

b / b;
b / (a - 2);
b / c;

c / c;
c / a;
c / b;

-- error

'apple' / 3;