--! qt:dataset:src
--! qt:dataset:alltypesorc

set hive.explain.user=false;
set hive.vectorized.execution.enabled=false;

explain extended prepare pcount from select count(*) from src where key > ?;
prepare pcount from select count(*) from src where key > ?;
execute pcount using 200;

-- single param
explain extended prepare p1 from select * from src where key > ? order by key limit 10;
prepare p1 from select * from src where key > ? order by key limit 10;

execute p1 using 200;

-- same query, different param
execute p1 using 0;

-- same query, negative param
--TODO: fails (constant in grammar do not support negatives)
-- execute p1 using -1;

-- boolean type
-- TODO: fails (UDFTOBoolean is added during prepare plan and boolean is bounded during execute, but UDFToBoolean do not support accepting boolean )
--explain prepare pbool from select count(*) from alltypesorc where cboolean1 = ? and cboolean2 = ? group by ctinyint;
--prepare pbool from select count(*) from alltypesorc where cboolean1 = ? and cboolean2 = ? group by ctinyint;
--execute pbool using true, false;
--select count(*) from alltypesorc where cboolean1 = true and cboolean2 = false group by ctinyint;

-- int, float, long and double type
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


