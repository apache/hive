create table t (a smallint, b string);
insert into t values (1, 'a');
insert into t values (2, 'aa');
insert into t values (6, 'aaaaaa');

select 1 from t where a in (1,2,3) and case a when 1 then true when 2 then true end;
explain cbo select 1 from t where a in (1,2,3) and case a when 1 then true when 2 then true end;

select 1 from t where a in (1,2,3) and case when a = 1 then true when a = 2 then true end;
explain cbo select 1 from t where a in (1,2,3) and case when a = 1 then true when a = 2 then true end;