create table t1 (key int, value int);

explain cbo
select * from 
(select key, value, BLOCK__OFFSET__INSIDE__FILE, INPUT__FILE__NAME, ROW__ID, ROW__IS__DELETED from t1) a join
(select key, value, BLOCK__OFFSET__INSIDE__FILE, INPUT__FILE__NAME, ROW__ID, ROW__IS__DELETED from t1) b join
(select key, value, BLOCK__OFFSET__INSIDE__FILE, INPUT__FILE__NAME, ROW__ID, ROW__IS__DELETED from t1) c;
