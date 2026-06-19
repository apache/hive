create table t3 (id int,name string, age int);
insert into t3 values(1,'Sagar',23),(2,'Sultan',NULL),(3,'Surya',23),(4,'Raman',45),(5,'Scott',23),(6,'Ramya',5),(7,'',23),(8,'',23),(9,'ron',3),(10,'Sam',22),(11,'nick',19),(12,'fed',18),(13,'kong',13),(14,'hela',45);

create table t4 (id int,name string, age int);
insert into t4 values(1,'Sagar',23),(3,'Surya',23),(4,'Raman',45),(5,'Scott',23),(6,'Ramya',5),(7,'',23),(8,'',23);

create table t5 (id int,name string, ages int);
insert into t5 values(1,'Sagar',23),(3,'Surya',NULL),(4,'Raman',45),(5,'Scott',23),(6,'Ramya',5),(7,'',23),(8,'',23);

set hive.cbo.enable = false;

select * from t3
where age in (select distinct(age) age from t4)
order by age ;

select * from t3
where age not in (select distinct(age) age from t4  )
order by age ;


select * from t3
where age not in (select distinct(ages) ages from t5 where t5.ages is not null)
order by age ;


select * from t3
where age not in (select distinct(ages) ages from t5 )
order by age ;

select count(*) from t3
where age not in (23,22, null );

explain select * from t3
        where age not in (select distinct(age) age from t4);

explain select * from t3
where age not in (select distinct(ages) ages from t5 );

explain select * from t3
        where age not in (select distinct(ages) ages from t5 where t5.ages is not null);

select count(*) from t3
where age not in (select distinct(age)age from t3 t1 where t1.age > 10);



explain select id, name, age
from t3 b where b.age not in
(select min(age)
 from (select id, age from t3) a
 where age < 10 and b.age = a.age)
 order by name;

set hive.cbo.enable = true;

select * from t3
where age in (select distinct(age) age from t4)
order by age ;

select * from t3
where age not in (select distinct(age) age from t4  )
order by age ;

select * from t3
where age not in (select distinct(ages) ages from t5 where t5.ages is not null)
order by age ;


select * from t3
where age not in (select distinct(ages) ages from t5 )
order by age ;

select count(*) from t3
where age not in (23,22, null );

explain select * from t3
        where age not in (select distinct(age) age from t4);

explain select * from t3
where age not in (select distinct(ages) ages from t5 );

explain select * from t3
        where age not in (select distinct(ages) ages from t5 where t5.ages is not null);

select count(*) from t3
where age not in (select distinct(age)age from t3 t1 where t1.age > 10);

 explain select id, name, age
         from t3 b where b.age not in
         (select min(age)
          from (select id, age from t3) a
          where age < 10 and b.age = a.age)
          order by name;
