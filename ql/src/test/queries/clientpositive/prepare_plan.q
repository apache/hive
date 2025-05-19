--! qt:dataset:src
--! qt:dataset:alltypesorc
--SORT_QUERY_RESULTS

set hive.explain.user=false;

-- single param
explain extended prepare pcount from select count(*) from src where key > ?;
prepare pcount from select count(*) from src where key > ?;
explain execute pcount using '200';
execute pcount using '200';

-- same query, different param
execute pcount using '0';

-- single param
explain prepare p1 from select * from src where key > ? order by key limit 10;
prepare p1 from select * from src where key > ? order by key limit 10;
explain execute p1 using '100';
execute p1 using '100';

-- same query, negative param
--TODO: fails (constant in grammar do not support negatives)
-- execute p1 using -1;

-- boolean type
-- TODO: fails (UDFTOBoolean is added during prepare plan and boolean is bounded during execute, but UDFToBoolean do not support accepting boolean )
--explain prepare pbool from select count(*) from alltypesorc where cboolean1 = ? and cboolean2 = ? group by ctinyint;
--prepare pbool from select count(*) from alltypesorc where cboolean1 = ? and cboolean2 = ? group by ctinyint;
--execute pbool using true, false;
--select count(*) from alltypesorc where cboolean1 = true and cboolean2 = false group by ctinyint;

-- query with group by
explain
    prepare pint
    from select avg(ctinyint) as ag from alltypesorc where cint <= ?  and cbigint <= ? and cfloat != ? group by ctinyint having ag < ?;
prepare pint
    from select avg(ctinyint) as ag from alltypesorc where cint <= ?  and cbigint <= ? and cfloat != ? group by ctinyint having ag < ?;

explain
    execute pint using 100, 5000000, 0.023, 0.0;
execute pint using 100, 5000000,0.023, 0.0;

-- tiny and short int
explain
    prepare psint
    from select count(*) as ag from alltypesorc where ctinyint = ?  and csmallint != ? group by cint ;

prepare psint
    from select count(*) as ag from alltypesorc where ctinyint <= ?  and csmallint != ? group by cint ;
explain
    execute psint using 3, 10;
execute psint using 3, 10;

-- char, varchar
create table tcharvchar(c char(10), v varchar(50)) stored as orc;
insert into tcharvchar values ('c1', 'v10'), ('c2', 'v100');

explain prepare pcharv  from select count(*) from tcharvchar where c = ? and v != ?;
prepare pcharv  from select count(*) from tcharvchar where c = ? and v != ?;

explain
    execute pcharv using 'c1', 'v1';
execute pcharv using 'c1', 'v1';
drop table tcharvchar;

-- date,timestamp
create table tdatets(t timestamp, d date, dc decimal(10,2)) stored as orc;
insert into tdatets values ( cast('2011-01-01 00:00:00' as timestamp), cast('1919-11-01' as date), 5.00);
insert into tdatets values ( cast('2010-01-01 04:00:00' as timestamp), cast('1918-11-01' as date), 4.00);

explain
    prepare ptsd from select count(*) from tdatets where t != ? and d != ? and dc > ?;
prepare ptsd from select count(*) from tdatets where t != ? and d != ? and dc > ?;

explain
    execute ptsd using '2012-01-01 00:01:01', '2020-01-01', 1.00;
execute ptsd using '2012-01-01 00:01:01', '2020-01-01', 1.00;
drop table tdatets;



-- multiple parameters
explain prepare p2 from select min(ctinyint), max(cbigint) from alltypesorc where cint > (? + ? + ?) group by ctinyint;
prepare p2 from select min(ctinyint), max(cbigint) from alltypesorc where cint > (? + ? + ?) group by ctinyint;

execute p2 using 0, 1, 2;

-- param in udf and plan with fetch task
explain prepare pconcat
    from select count(*) from src where key > concat(?, ?);
prepare pconcat
    from select count(*) from src where key > concat(?, ?);
explain
    execute pconcat using '1','20';
execute pconcat using '1','20';

-- partitioned table
create table daysales (customer int) partitioned by (dt string);

insert into daysales partition(dt='2001-01-01') values(1);
insert into daysales partition(dt='2001-01-03') values(1);
insert into daysales partition(dt='2001-01-03') values(1);

explain prepare pPart1 from select count(*) from daysales where dt=? and customer=?;
prepare pPart1 from select count(*) from daysales where dt=? and customer=?;

explain execute pPart1 using '2001-01-01',1;
-- count should be 1
execute pPart1 using '2001-01-01',1;

--count should be 2
execute pPart1 using '2001-01-03', 1;


-- all data types with all possible filters
-- TODO: boolean, timestamptz, interval

create table testParam(c char(5), v varchar(10), d decimal(10,3), dt date) stored as textfile;
insert into testParam values ('ch1', 'var1', 1000.34,'1947-12-12' );
insert into testParam values ('ch2', 'var2', 2000.00,'1967-02-02');

CREATE TABLE alltypes(
    c char(5),
    v varchar(10),
    d decimal(10,3),
    dt date,
    bl boolean,
    ctinyint TINYINT,
    csmallint SMALLINT,
    cint INT,
    cbigint BIGINT,
    cfloat FLOAT,
    cdouble DOUBLE,
    cstring1 STRING,
    ctimestamp1 TIMESTAMP)
    stored as textfile ;

insert into alltypes select c,v,d,dt,cboolean1,ctinyint,csmallint,cint,cbigint,cfloat,cdouble,cstring1,ctimestamp1
    from testParam join alltypesorc where ctinyint is not null;

-- greater than
explain prepare palltypesGreater from
select count(*) from alltypes where c > ? OR v > ? OR d > ? OR dt > ? OR ctinyint > ? OR csmallint > ? OR cint > ?
 OR cfloat > ? OR cdouble > ? OR cstring1 > ? OR ctimestamp1 > ? OR cbigint > ?;

prepare palltypesGreater from
select count(*) from alltypes where c > ? OR v > ? OR d > ? OR dt > ? OR ctinyint > ? OR csmallint > ? OR cint > ?
                                 OR cfloat > ? OR cdouble > ? OR cstring1 > ? OR ctimestamp1 > ? OR cbigint > ?;

explain execute palltypesGreater using 'a','v',1000.00,'1954-12-12',0,7476,528534766,24.00,5780.3,'cvLH6Eat2yFsyy','1968-12-31 15:59:46.674',0;
execute palltypesGreater using 'a','v',1000.00,'1954-12-12',0,7476,528534766,24.00,5780.3,'cvLH6Eat2yFsyy','1968-12-31 15:59:46.674',0;

-- less than
explain prepare palltypesLess from
select count(*) from alltypes where c < ? OR v < ? OR d < ? OR dt < ? OR ctinyint < ? OR csmallint < ? OR cint < ?
                                 OR cfloat < ? OR cdouble < ? OR cstring1 < ? OR ctimestamp1 < ? OR cbigint < ?;

prepare palltypesLess from
select count(*) from alltypes where c < ? OR v < ? OR d < ? OR dt < ? OR ctinyint < ? OR csmallint < ? OR cint < ?
                                 OR cfloat < ? OR cdouble < ? OR cstring1 < ? OR ctimestamp1 < ? OR cbigint < ?;

explain execute palltypesGreater using 'd','z',10000.00,'1995-12-12',0,7476,528534766,24.00,5780.3,'cvLH6Eat2yFsyy','1968-12-31 15:59:46.674',0;
execute palltypesGreater using 'd','z',10000.00,'1995-12-12',0,7476,528534766,24.00,5780.3,'cvLH6Eat2yFsyy','1968-12-31 15:59:46.674',0;

--equality
explain prepare pequal from
select count(*) from alltypes where c = ? OR v = ? OR d = ? OR dt = ? OR ctinyint = ? OR csmallint = ? OR cint = ?
                                 OR cfloat = ? OR cdouble = ? OR cstring1 = ? OR ctimestamp1 = ? OR cbigint = ?;

prepare pequal from
select count(*) from alltypes where c = ? OR v = ? OR d = ? OR dt = ? OR ctinyint = ? OR csmallint = ? OR cint = ?
                                 OR cfloat = ? OR cdouble = ? OR cstring1 = ? OR ctimestamp1 = ? OR cbigint = ?;

explain execute pequal using 'ch1','var1',1000.34,'1947-12-12',11,0,529436599,1.0,1.400,'xTlDv24JYv4s','1969-12-31 16:00:02.351',133;
execute pequal using 'ch1','var1',1000.34,'1947-12-12',11,0,529436599,1.0,1.400,'xTlDv24JYv4s','1969-12-31 16:00:02.351',133;

-- IN
explain prepare pin from
select count(*) from alltypes where c IN(?,?) AND  v IN(?, ?) AND d IN (?,?) AND dt IN (?) OR ctinyint IN (?) AND csmallint IN(?,?,?) AND cint IN(?,?,?)
    AND cfloat IN(?,?) AND cdouble IN(?,?,?) OR cstring1 IN (?,?,?)  AND ctimestamp1 IN (?) OR cbigint IN (?);

prepare pin from
select count(*) from alltypes where c IN(?,?) AND  v IN(?, ?) AND d IN (?,?) AND dt IN (?) OR ctinyint IN (?) AND csmallint IN(?,?,?) AND cint IN(?,?,?)
    AND cfloat IN(?,?) AND cdouble IN(?,?,?) OR cstring1 IN (?,?,?)  AND ctimestamp1 IN (?) OR cbigint IN (?);

explain execute pin using 'ch1','ch2','var1', 'var2',1000.34,2000.00, '1947-12-12',11 ,15601,0,1,788564623,78856,23,1.0,18.00,0,15601.0,23.1,'xTlDv24JYv4s','str1','stre','1969-12-31 16:00:02.351',133;
execute pin using 'ch1','ch2','var1', 'var2',1000.34,2000.00, '1947-12-12',11 ,15601,0,1,788564623,78856,23,1.0,18.00,0,15601.0,23.1,'xTlDv24JYv4s','str1','stre','1969-12-31 16:00:02.351',133;


-- BETWEEN
explain prepare pbetween from
    select count(*) from alltypes where (c BETWEEN ? AND ?) AND (v BETWEEN ? AND ?) AND (d BETWEEN ? AND ?) AND (dt BETWEEN ? AND ?) OR (ctinyint BETWEEN ? AND ?) AND (csmallint BETWEEN ? AND ?) AND (cint BETWEEN ? AND ?)
    AND (cfloat BETWEEN ? AND ?) AND (cdouble BETWEEN ? AND ?) OR (cstring1 BETWEEN ? AND ?)  AND (ctimestamp1 BETWEEN ? AND ?) OR (cbigint BETWEEN ? AND ?);

prepare pbetween from
select count(*) from alltypes where (c BETWEEN ? AND ?) AND (v BETWEEN ? AND ?) AND (d BETWEEN ? AND ?) AND (dt BETWEEN ? AND ?) OR (ctinyint BETWEEN ? AND ?) AND (csmallint BETWEEN ? AND ?) AND (cint BETWEEN ? AND ?)
    AND (cfloat BETWEEN ? AND ?) AND (cdouble BETWEEN ? AND ?) OR (cstring1 BETWEEN ? AND ?)  AND (ctimestamp1 BETWEEN ? AND ?) OR (cbigint BETWEEN ? AND ?);

explain execute pbetween using 'ch1' ,'ch2' ,'var1' ,'var2',1000.34, 2000.0, '1947-12-12', '1968-12-31', 11, 1000, 15601, 1, 788564623, 23,1.0, 18.00, 0, 15601.0, 'xTlDv24JYv4s', 'str1', '1969-12-31 16:00:02.351','2020-12-31 16:00:01', 0, 133;
execute pbetween using 'ch1' ,'ch2' ,'var1' ,'var2',1000.34, 2000.0, '1947-12-12', '1968-12-31', 11, 1000, 15601, 1, 788564623, 23,1.0, 18.00, 0, 15601.0, 'xTlDv24JYv4s', 'str1', '1969-12-31 16:00:02.351','2020-12-31 16:00:01', 0, 133;

DROP TABLE testParam;
DROP TABLE alltypes;
