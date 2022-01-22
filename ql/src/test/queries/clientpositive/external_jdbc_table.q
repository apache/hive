--! qt:disabled:HIVE-25379
--! qt:dataset:src

set hive.strict.checks.cartesian.product= false;


CREATE TABLE simple_hive_table1 (ikey INT, bkey BIGINT, fkey FLOAT, dkey DOUBLE );

CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';


FROM src

SELECT

dboutput ( 'jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'CREATE TABLE SIMPLE_DERBY_TABLE1 ("ikey" INTEGER, "bkey" BIGINT, "fkey" REAL, "dkey" DOUBLE)' ),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE1 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','20','20','20.0','20.0'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE1 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','-20','-20','-20.0','-20.0'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE1 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','100','-15','65.0','-74.0'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE1 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','44','53','-455.454','330.76')

limit 1;

FROM src

SELECT

dboutput ( 'jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'CREATE TABLE SIMPLE_DERBY_TABLE2 ("ikey" INTEGER, "bkey" BIGINT, "fkey" REAL, "dkey" DOUBLE, "datekey" DATE)' ),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE2 ("ikey","bkey","fkey","dkey","datekey") VALUES (?,?,?,?,?)','20','20','20.0','20.0','1999-02-22'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE2 ("ikey","bkey","fkey","dkey","datekey") VALUES (?,?,?,?,?)','-20','8','9.0','11.0','2000-03-15'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE2 ("ikey","bkey","fkey","dkey","datekey") VALUES (?,?,?,?,?)','101','-16','66.0','-75.0','2010-04-01'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE2 ("ikey","bkey","fkey","dkey","datekey") VALUES (?,?,?,?,?)','40','50','-455.4543','330.767','2010-04-02')

limit 1;


CREATE EXTERNAL TABLE ext_simple_derby_table1
(
 ikey int,
 bkey bigint,
 fkey float,
 dkey double
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "SIMPLE_DERBY_TABLE1",
                "hive.sql.dbcp.maxActive" = "1"
);


CREATE EXTERNAL TABLE ext_simple_derby_table2
(
 ikey int,
 bkey bigint,
 fkey float,
 dkey double,
 datekey string
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "SIMPLE_DERBY_TABLE2",
                "hive.sql.dbcp.maxActive" = "1"
);


select * from ext_simple_derby_table1;

--Test projection
select dkey,fkey,bkey,ikey from ext_simple_derby_table1;
select bkey+ikey,fkey+dkey from ext_simple_derby_table1;
select abs(dkey),abs(ikey),abs(fkey),abs(bkey) from ext_simple_derby_table1;

select datekey from ext_simple_derby_table2;

--Test aggregation
select count(*) from ext_simple_derby_table1;
select count(distinct bkey) from ext_simple_derby_table1;
select count(ikey), sum(bkey), avg(dkey), max(fkey) from ext_simple_derby_table1;


--Test sort
select dkey from ext_simple_derby_table1 order by dkey;
select SUM_IKEY,bkey from (select sum(-ikey) as SUM_IKEY, bkey from ext_simple_derby_table1 group by bkey) ttt order by bkey;

--Test filter
explain select bkey from ext_simple_derby_table1 where 100 < ext_simple_derby_table1.ikey;
select bkey from ext_simple_derby_table1 where 100 < ext_simple_derby_table1.ikey;

SELECT distinct dkey from ext_simple_derby_table1 where ikey = '100';
SELECT count(*) FROM (select * from ext_simple_derby_table1) v WHERE ikey = 100;
SELECT count(*) from ext_simple_derby_table1 having count(*) > 0;
select sum(8),8 from ext_simple_derby_table1 where ikey = 1 group by 2;


--Test join
explain select ext_simple_derby_table1.fkey, ext_simple_derby_table2.dkey from ext_simple_derby_table1 join ext_simple_derby_table2 on
(ext_simple_derby_table1.ikey = ext_simple_derby_table2.ikey);

select ext_simple_derby_table1.fkey, ext_simple_derby_table2.dkey from ext_simple_derby_table1 join ext_simple_derby_table2 on
(ext_simple_derby_table1.ikey = ext_simple_derby_table2.ikey);


explain select simple_hive_table1.fkey, ext_simple_derby_table2.dkey from simple_hive_table1 join ext_simple_derby_table2 on
(simple_hive_table1.ikey = ext_simple_derby_table2.ikey);

select simple_hive_table1.fkey, ext_simple_derby_table2.dkey from simple_hive_table1 join ext_simple_derby_table2 on
(simple_hive_table1.ikey = ext_simple_derby_table2.ikey);


--Test union

SELECT ikey FROM simple_hive_table1
UNION
SELECT bkey FROM ext_simple_derby_table2;


-- CBO explain
explain cbo
select ext_simple_derby_table1.fkey, ext_simple_derby_table2.dkey from ext_simple_derby_table1 join ext_simple_derby_table2 on
(ext_simple_derby_table1.ikey = ext_simple_derby_table2.ikey);




----FAILURES----

--The following does not work due to invalid generated derby syntax:
--SELECT "dkey", COUNT("bkey") AS "$f1" FROM "SIMPLE_DERBY_TABLE1" GROUP BY "dkey" OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY {LIMIT 1}

--SELECT  dkey,count(bkey) from ext_simple_derby_table1 group by dkey limit 10;





--Fails parse.CalcitePlanner: CBO failed, skipping CBO.
--select sum(fkey) from ext_simple_derby_table1 where bkey in (10, 100);




--Fails to ClassCastException
--




--SELECT ikey FROM ext_simple_derby_table1
--UNION
--SELECT bkey FROM ext_simple_derby_table2;



--Fails due to cast exception in SqlImplementor line 539:
--select sum(bkey) from ext_simple_derby_table1 where ikey = 2450894 OR ikey = 2450911;



--select dkey from ext_simple_derby_table1 order by dkey limit 10 offset 60;
