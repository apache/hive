create table t1 (key int, value int);

explain cbo
with cte as
(select key, value, BLOCK__OFFSET__INSIDE__FILE, INPUT__FILE__NAME, ROW__ID, ROW__IS__DELETED from t1)
select * from cte a join cte b join cte c 
