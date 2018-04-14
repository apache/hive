--! qt:dataset:src
--create hive hbase table 1
drop table if exists hive1_tbl_data_hbase1;
drop table if exists hive1_tbl_data_hbase2;
drop view if exists hive1_view_data_hbase1;
drop view if exists hive1_view_data_hbase2;

CREATE EXTERNAL TABLE hive1_tbl_data_hbase1 (COLUMID string,COLUMN_FN string,COLUMN_LN string,EMAIL string,COL_UPDATED_DATE timestamp, PK_COLUM string) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES("hbase.columns.mapping" = "default:COLUMID,default:COLUMN_FN,default:COLUMN_LN,default:EMAIL,default:COL_UPDATED_DATE,:key" 
)
TBLPROPERTIES ("external.table.purge" = "true");

--create hive view for the above hive table 1
CREATE VIEW hive1_view_data_hbase1 
AS 
SELECT * 
FROM hive1_tbl_data_hbase1
WHERE PK_COLUM >='4000-00000'
and PK_COLUM <='4000-99999'
AND COL_UPDATED_DATE IS NOT NULL
;


--load data to hive table 1
insert into table hive1_tbl_data_hbase1 select '00001','john','doe','john@hotmail.com','2014-01-01 12:01:02','4000-10000' from src where key = 100;

--create hive hbase table 2
CREATE EXTERNAL TABLE hive1_tbl_data_hbase2 (COLUMID string,COLUMN_FN string,COLUMN_LN string,EMAIL string,COL_UPDATED_DATE timestamp, PK_COLUM string) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES("hbase.columns.mapping" = "default:COLUMID,default:COLUMN_FN,default:COLUMN_LN,default:EMAIL,default:COL_UPDATED_DATE,:key" 
)
TBLPROPERTIES ("external.table.purge" = "true");

--create hive view for the above hive hbase table 2
CREATE VIEW hive1_view_data_hbase2 
AS 
SELECT * 
FROM hive1_tbl_data_hbase2 
where COL_UPDATED_DATE IS NOT NULL
;


--load data to hive hbase table 2
insert into table hive1_tbl_data_hbase2 select '00001','john','doe','john@hotmail.com','2014-01-01 12:01:02','00001' from src where key = 100; 
;

set hive.optimize.ppd = true;
set hive.auto.convert.join=false;

-- do not return value without fix 

select x.FIRST_NAME1, x.EMAIL1 from (
select p.COLUMN_FN as first_name1, a.EMAIL as email1 from hive1_view_data_hbase2 p inner join hive1_view_data_hbase1 a on p.COLUMID =a.COLUMID) x;

set hive.auto.convert.join=true;

-- return value with/without fix

select x.FIRST_NAME1, x.EMAIL1 from (
select p.COLUMN_FN as first_name1, a.EMAIL as email1 from hive1_view_data_hbase2 p inner join hive1_view_data_hbase1 a on p.COLUMID =a.COLUMID) x;
 
