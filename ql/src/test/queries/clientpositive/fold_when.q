--! qt:dataset:src
explain
select key from src where ((case when (key = '238') then null     end) = 1);
explain
select key from src where ((case when (key = '238') then null else null end) = 1);
explain
select key from src where ((case when (key = '238') then 1 else 1 end) = 1);
explain
select key from src where ((case when (key = '238') then 1 else 1 end) = 2);
explain
select key from src where ((case when (key = '238') then 1 else null end) = 1);
explain
select key from src where ((case when (key = '238') then 1=1 else null=1 end));
explain
select key from src where ((case when (key = '238') then 1=1 else 2=2 end));
explain
select key from src where ((case when (key = '238') then 1=3 else 2=1 end));
explain
select key from src where ((case when (key = '238') then 1=1 else 2=1 end));
explain
select key from src where ((case when (key = '238') then 1=3 else 1=1 end));
explain
select key from src where ((case when ('23' = '23') then 1 else 1 end) = 1);
explain
select key from src where ((case when ('2' = '238') then 1 else 2 end) = 2);
explain
select key from src where ((case when (true=null) then 1 else 1 end) = 1);
explain
select key from src where ((case when (key = (case when (key = '238') then '11' else '11'  end)) then false else true end));
explain
select key from src where ((case when (key = (case when (key = '238') then '12' else '11'  end)) then 2=2   else true end));

