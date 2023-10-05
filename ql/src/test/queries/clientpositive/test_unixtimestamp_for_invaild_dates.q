DESCRIBE FUNCTION UNIX_TIMESTAMP;
set hive.datetime.formatter=DATETIME;


CREATE TABLE DATETIMETABLE (month STRING, dateTimeStamp STRING);

-- Insert valid and invalid dates
INSERT INTO DATETIMETABLE VALUES ('Feb', '2001-02-28'), ('Feb','2001-02-29'),('Feb','2001-02-30'), ('Feb','2001-02-31'),
('Feb', '2001-02-32');

INSERT INTO DATETIMETABLE VALUES('Apr', '2001-04-30'), ('Apr', '2001-04-31'), ('Apr','2001-04-32');

INSERT INTO DATETIMETABLE VALUES('Jun', '2001-06-30'),('Jun', '2001-06-31'), ('Jun','2001-06-32');

INSERT INTO DATETIMETABLE VALUES('Spet', '2001-09-30'),('Sept', '2001-09-31'), ('Sept','2001-09-32');

INSERT INTO DATETIMETABLE VALUES('Nov', '2001-11-30'),('Nov', '2001-11-31'), ('Nov','2001-11-32');

select month, datetimestamp, unix_timestamp(datetimestamp, 'uuuu-MM-dd') as timestampCol from datetimetable;
select month, datetimestamp, unix_timestamp(datetimestamp, 'yyyy-MM-dd') as timestampCol from datetimetable;
