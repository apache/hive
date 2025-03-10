CREATE TABLE t1 (col1 char(3));

INSERT INTO t1 VALUES ('A'),('b'),('c'),(null);

explain cbo
select col1, case upper(col1) when 'A' then 'OK' else 'N/A' end as col2 from t1;
explain
select col1, case upper(col1) when 'A' then 'OK' else 'N/A' end as col2 from t1;

select col1, case upper(col1) when 'A' then 'OK' else 'N/A' end as col2 from t1;

explain cbo
select col1, case when upper(col1)='A' then 'OK' else 'N/A' end as col2 from t1;
explain
select col1, case when upper(col1)='A' then 'OK' else 'N/A' end as col2 from t1;

select col1, case when upper(col1)='A' then 'OK' else 'N/A' end as col2 from t1;

explain cbo
select col1,
  case lower(col1)
    when 'a' then 'OK a'
    when 'b' then 'OK b'
    else 'N/A'
    end as col2 from t1;

explain cbo
select col1,
  case
    when lower(col1) = 'a' then 'OK a'
    when lower(col1) = 'b' then 'OK b'
    else 'N/A'
    end as col2 from t1;

select col1,
  case lower(col1)
    when 'a' then 'OK a'
    when 'b' then 'OK b'
    else 'N/A'
    end as col2 from t1;
