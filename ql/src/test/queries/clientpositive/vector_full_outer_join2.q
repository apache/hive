create table t_letters (z char(12), x int, y int);

insert into t_letters values
('one', 1, 50),
('two', 2, 30),
('three', 3, 30),
('four', 4, 60),
('five', 5, 70),
('six', 6, 80);

create table t_roman (z char(12), x int, y int);

insert into t_roman values
('I', 1, 50),
('II', 2, 30),
('III', 3, 30),
('IV', 4, 60),
('V', 5, 70),
('VI', 6, 80);


select x1.`z`, x1.`x`, x1.`y`,
       x2.`z`, x2.`x`, x2.`y`
from t_letters x1 full outer join t_roman x2 on (x1.`x` > 3) and (x2.`x` < 4) and (x1.`x` = x2.`x`);

select x1.`z`, x1.`x`, x1.`y`,
       x2.`z`, x2.`x`, x2.`y`
from t_letters x1 full outer join t_roman x2 on (x1.`x` > 2) and (x2.`x` < 4) and (x1.`x` = x2.`x`);