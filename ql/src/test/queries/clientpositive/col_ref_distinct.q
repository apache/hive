create table t1 (a bigint, b int);

insert into t1(a, b) values (1, 1), (1, 1), (1, NULL), (2, 2);

-- column referenced by its name
explain cbo
select distinct b as c1 from t1 order by b;
select distinct b as c1 from t1 order by b;

-- column referenced by its name upper case
explain cbo
select distinct b as c1 from t1 order by B;

-- column referenced by its alias
explain cbo
select distinct b as c1 from t1 order by c1;

-- column referenced by its alias upper case
explain cbo
select distinct b as c1 from t1 order by C1;

-- order by expression
select distinct b + a as c1 from t1 order by b + a;
