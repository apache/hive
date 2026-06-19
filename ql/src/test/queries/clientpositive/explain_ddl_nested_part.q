set hive.exec.dynamic.partition=true;

create table test(foo int) partitioned by  (i int, j int);
create table temp (i int);
insert into temp values (1),(2),(3),(4),(5),("ab"),("cd");

insert into test partition(i, i)
select i, i, i from temp;

explain ddl select * from test;

create table test2 (foo int) partitioned by (i int, j int, s string, t string);

create table temp2 (i int, s string);

insert into temp2 values (1880938,"1880938"),(3023419,"3023419"),(42629,"42629"),(3073641,"3073641"),(6758530,"6758530"),(3847717,"3847717"),(7880229,"7880229"),(4497880,"4497880"),(1956691,"1956691"),(2146641,"2146641"),(7005576,"7005576"),(1522484,"1522484"),(3076080,"3076080"),(4409637,"4409637"),(812837,"812837"),(967815,"967815"),(5247332,"5247332"),(7670676,"7670676"),(4604522,"4604522"),(9805326,"9805326"),(3583871,"3583871"),(6276003,"6276003"),(3187683,"3187683"),(7286712,"7286712"),(5238051,"5238051"),(4439186,"4439186"),(3356420,"3356420"),(2581458,"2581458"),(1565998,"1565998"),(4115101,"4115101"),(6129618,"6129618"),(4304398,"4304398"),(2049769,"2049769"),(5803231,"5803231"),(1655195,"1655195"),(971079,"971079");
insert into temp2 values (1, NULL), (2,NULL), (NULL,NULL), (NULL,"3");

insert into test2 partition(i,i,s,s)
select i,i,i,s,s from temp2;

explain ddl select * from test2;