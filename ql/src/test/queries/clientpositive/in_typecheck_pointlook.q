
create table customer_demographics (cd_marital_status char(1), cd_education_status char(20));

insert into customer_demographics values
('M','Unknown'),
('W','Advanced Degree'),
('W','Advanced Degree '),
('W',' Advanced Degree')
;

set hive.optimize.point.lookup.min=32;

explain
select count(1)
from customer_demographics
where ( (cd_marital_status = 'M' and cd_education_status = 'Unknown')
or      (cd_marital_status = 'W' and cd_education_status = 'Advanced Degree'));

select '3 is expected:',count(1) 
from customer_demographics
where ( (cd_marital_status = 'M' and cd_education_status = 'Unknown')
or      (cd_marital_status = 'W' and cd_education_status = 'Advanced Degree'));

set hive.optimize.point.lookup.min=2;

explain
select count(1)
from customer_demographics
where ( (cd_marital_status = 'M' and cd_education_status = 'Unknown')
or      (cd_marital_status = 'W' and cd_education_status = 'Advanced Degree'));

select '3 is expected:',count(1) 
from customer_demographics
where ( (cd_marital_status = 'M' and cd_education_status = 'Unknown')
or      (cd_marital_status = 'W' and cd_education_status = 'Advanced Degree'));
