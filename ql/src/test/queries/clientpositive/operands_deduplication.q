create table mytable(id int, name string);

explain cbo select t1.id, t2.name, (case when (t1.id = 1 and t2.id = 1) then 'One' else null end) mycase
from mytable t1 join mytable t2 on t1.name = t2.name where t1.id =1 and t2.id = 1;
