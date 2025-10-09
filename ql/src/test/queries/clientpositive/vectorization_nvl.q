drop table if exists esource;
drop table if exists etarget;

create table if not exists esource_txt (
 client_id string,
   id_enddate decimal(10,0),                     
   client_gender string,                         
   birthday decimal(10,0),                       
   nationality string,
   address_zipcode string,                       
   income decimal(15,2),                         
   address string,
   part_date int
) row format delimited fields terminated by '|'
 lines terminated by '\n' stored as textfile;

create table esource like esource_txt;
alter table esource set fileformat orc;

load data local inpath '../../data/files/esource.txt' overwrite into table esource_txt;

insert overwrite table esource select * from esource_txt;

Select * from esource where part_date = 20230414;

CREATE EXTERNAL TABLE etarget(                   
   client_id string,
   id_enddate decimal(10,0),                     
   client_gender string,                         
   birthday decimal(10,0),                       
   nationality string,
   address_zipcode string,                       
   income decimal(15,2),                         
   address string,
   part_date int,
   bdata_no int)
 CLUSTERED BY (                                    
   bdata_no)                                       
 INTO 1 BUCKETS                                    
 ROW FORMAT SERDE                                  
   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'     
 STORED AS INPUTFORMAT                             
   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
 OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';


 insert overwrite table etarget
select mt.*, floor(rand() * 1) as bdata_no from (select nvl(np.client_id,' '),nvl(np.id_enddate,cast(0 as decimal(10,0))),nvl(np.client_gender,' '),nvl(np.birthday,cast(0 as decimal(10,0))),nvl(np.nationality,' '),nvl(np.address_zipcode,' '),nvl(np.income,cast(0 as decimal(15,2))),nvl(np.address,' '),nvl(np.part_date,cast(0 as int)) from (select * from esource where part_date = 20230414) np) mt;

select client_id,birthday,income from etarget;
