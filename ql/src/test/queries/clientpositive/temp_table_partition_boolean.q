-- SORT_QUERY_RESULTS

CREATE TEMPORARY TABLE broken_temp (c int) PARTITIONED BY (b1 BOOLEAN, s STRING, b2 BOOLEAN, i INT);

-- Insert a few variants of 'false' partition-key values.;
INSERT INTO TABLE broken_temp PARTITION(b1=false,s='a',b2=false,i=0) VALUES(1);
INSERT INTO TABLE broken_temp PARTITION(b1=FALSE,s='a',b2=false,i=0) VALUES(3);
INSERT INTO TABLE broken_temp PARTITION(b1='no',s='a',b2=False,i=0) VALUES(5);
INSERT INTO TABLE broken_temp PARTITION(b1='off',s='a',b2='0',i=0) VALUES(7);

select * from broken_temp where b1=false and b2=false;

-- Insert a few variants of 'true' partition-key values.;
INSERT INTO TABLE broken_temp PARTITION(b1=true,s='a',b2=true,i=0) VALUES(2);
INSERT INTO TABLE broken_temp PARTITION(b1=TRUE,s='a',b2=true,i=0) VALUES(4);
INSERT INTO TABLE broken_temp PARTITION(b1='yes',s='a',b2=True,i=0) VALUES(6);
INSERT INTO TABLE broken_temp PARTITION(b1='1',s='a',b2='on',i=0) VALUES(8);

select * from broken_temp where b1 is true and b2 is true;

-- Insert a few variants of mixed 'true'/'false' partition-key values.;
INSERT INTO TABLE broken_temp PARTITION(b1=false,s='a',b2=true,i=0) VALUES(100);
INSERT INTO TABLE broken_temp PARTITION(b1=FALSE,s='a',b2=TRUE,i=0) VALUES(1000);
INSERT INTO TABLE broken_temp PARTITION(b1=true,s='a',b2=false,i=0) VALUES(10000);
INSERT INTO TABLE broken_temp PARTITION(b1=tRUe,s='a',b2=fALSe,i=0) VALUES(100000);

select * from broken_temp where b1 is true and b2=false;
select * from broken_temp where b1=false and b2 is true;

select count(*) from broken_temp;
select * from broken_temp;

show partitions broken_temp;

