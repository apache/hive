add jar /home/msydoron/eclipse-workspace/JethroDataJDBCDriver/target/jethro-jdbc-3.6-standalone.jar;


create table hivetable_1(x1 int, y1 double);    

INSERT INTO hivetable_1
VALUES
(9,9.0),
(99,99.0),
(999,999.0),
(9999,9999.0), 
(99999,99999.0);

CREATE EXTERNAL TABLE ext_mytable1 (x1 INT, y1 DOUBLE)
STORED BY
'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES ( "hive.sql.database.type" = "JETHRO_DATA",
                "hive.sql.jdbc.driver" = "com.jethrodata.JethroDriver",
                "hive.sql.jdbc.url" = "jdbc:JethroData://10.0.0.221:9111/demo3",
                "hive.sql.dbcp.username" = "jethro",
                "hive.sql.dbcp.password" = "jethro", 
                "hive.sql.table" = "mytable1",
                "hive.sql.dbcp.maxActive" = "1");



--insert into ext_mytable1 select * from hivetable_1 where 11<x1;



--CREATE TABLE jethro_table_1
--STORED BY
--'org.apache.hive.storage.jdbc.JdbcStorageHandler'
--TBLPROPERTIES ( "hive.sql.database.type" = "JETHRO_DATA",
--                "hive.sql.jdbc.driver" = "com.jethrodata.JethroDriver",
--                "hive.sql.jdbc.url" = "jdbc:JethroData://10.0.0.221:9111/demo3",
--                "hive.sql.dbcp.username" = "jethro",
--                "hive.sql.dbcp.password" = "jethro", 
----                "hive.sql.table" = "mytable1",
--                "hive.sql.dbcp.maxActive" = "1")
--AS select * from hivetable_1;   



CREATE EXTERNAL TABLE ext_mytable2 (x2 INT, y2 DOUBLE)
STORED BY
'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES ( "hive.sql.database.type" = "JETHRO_DATA",
                "hive.sql.jdbc.driver" = "com.jethrodata.JethroDriver",
                "hive.sql.jdbc.url" = "jdbc:JethroData://10.0.0.221:9111/demo3",
                "hive.sql.dbcp.username" = "jethro",
                "hive.sql.dbcp.password" = "jethro", 
                "hive.sql.table" = "mytable2",
                "hive.sql.dbcp.maxActive" = "1");





SELECT ext_mytable1.x1, ext_mytable1.y1, ext_mytable2.x2
FROM ext_mytable1
JOIN ext_mytable2 ON ext_mytable1.x1=ext_mytable2.x2 where (sqrt(x1*y1)   = sqrt(x2*y2) and bround (x1) != sqrt (y1)) and 
														   sqrt(x1*x2)   = sqrt(y1*y2) and
 													       bround(x1*y1) = bround(x2*y2);


--SELECT abs (ext_mytable1.x1), ext_mytable1.y1 FROM ext_mytable1 where bround (x1) +1 = 8;
----SELECT x1,y1 FROM ext_mytable1 limit 3 offset 1;
--SELECT x1, sum(y1*8.0) FROM ext_mytable1 group by x1 order by sum(y1*8);
--SELECT x1 FROM ext_mytable1 order by y1;
--SELECT sum(y1*8.0) FROM ext_mytable1 group by x1 order by sum(y1*8);
--
--SELECT x1,y1 FROM ext_mytable1 order by y1 limit 3 offset 1;
--SELECT x1 FROM ext_mytable1 order by y1 limit 3;
--
--select count (*) from ext_mytable2;
--
--select count (x1) from ext_mytable1;






SELECT count (ext_mytable1.x1) FROM ext_mytable1;



SELECT ext_mytable1.x1, ext_mytable1.y1, ext_mytable2.x2
FROM ext_mytable1
JOIN ext_mytable2 ON ext_mytable1.x1=ext_mytable2.x2 and sqrt(x1*y1)   = sqrt(x2*y2) and 
														 sqrt(x1*x2)   = sqrt(y1*y2) and
 													     bround(x1*y1) = bround(x2*y2);
 													       

 													       
select y1,x1,sqrt(x1) from ext_mytable1 where bround(x1) + 1 = sqrt(y1) and x1*y1 = sqrt(y1*x1*y1) and x1+y1 = y1-x1; 

SELECT ext_mytable1.x1, ext_mytable1.y1, ext_mytable2.x2
FROM ext_mytable1
INNER JOIN ext_mytable2 ON ext_mytable1.x1=ext_mytable2.x2 and ext_mytable1.y1=ext_mytable2.y2 and ext_mytable1.x1=10;



                
--select count(*) from ext_mytable where x=10 group by x;
--
--select count(x) from ext_mytable where x!=10;
--select y,x from ext_mytable where x=10;
----select x,count(*) from ext_mytable where x=10;
--select x,y from ext_mytable where bround(x*y)=10;
--
--
--select x,y*y from ext_mytable where x*x!=100;
--
--select x, count(x) from ext_mytable where x!=10 group by x;
--
----select x, count(*) from ext_mytable where x==10 group by x;
--




