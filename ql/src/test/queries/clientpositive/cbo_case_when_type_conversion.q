CREATE EXTERNAL TABLE t1 (col1 char(3));

INSERT INTO t1 VALUES ('A'),('b'),('c'),(null);

explain cbo
select col1, case upper(col1) when 'A' then 'OK' else 'N/A' end as col2 from t1;
explain
select col1, case upper(col1) when 'A' then 'OK' else 'N/A' end as col2 from t1;

select col1, case upper(col1) when 'A' then 'OK' else 'N/A' end as col2 from t1;

select col1,
  case
    when 'b' <> lower(col1) then 'not OK'
    when 'b' = lower(col1) then 'OK'
    else 'N/A'
    end as col2 from t1;
