create table t1 (id int);
create table t2 (id int);

explain cbo select id,
  (select count(*) from t1 join t2 on t1.id=t2.id)
  from t2
order by id;

explain cbo select id,
  (select count(*) from t1 join t2 using (id))
  from t2
order by id;

explain cbo select id,
  (select count(*) from t1 join t2 where t1.id=t2.id)
  from t2
order by id;
