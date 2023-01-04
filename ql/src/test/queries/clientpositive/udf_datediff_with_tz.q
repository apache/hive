--! qt:timezone:GMT+8

--create test data
create external table test_dt(id string, dt date);
insert into test_dt values('11', '2021-07-06'), ('22', '2021-07-07');

--test all the three variants of datediff() - (col,scalar), (scalar, col), (col, col)
select datediff(dt1.dt, '2021-07-01') from test_dt dt1;
select datediff(dt1.dt, '2021-07-01') from test_dt dt1 left join test_dt dt on dt1.id = dt.id;

select datediff('2021-07-01', dt1.dt) from test_dt dt1;
select datediff('2021-07-01', dt1.dt) from test_dt dt1 left join test_dt dt on dt1.id = dt.id;

select datediff(dt1.dt, dt1.dt) from test_dt dt1;
select datediff(dt1.dt, dt1.dt) from test_dt dt1 left join test_dt dt on dt1.id = dt.id;