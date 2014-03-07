set hive.auto.convert.join=true;

explain 
select s1.key as key, s1.value as value from src s1 join src s3 on s1.key=s3.key
UNION  ALL  
select s2.key as key, s2.value as value from src s2;

select s1.key as key, s1.value as value from src s1 join src s3 on s1.key=s3.key
UNION  ALL  
select s2.key as key, s2.value as value from src s2;