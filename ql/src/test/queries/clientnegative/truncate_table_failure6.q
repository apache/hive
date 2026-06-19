create table external1 (col1 int, col2 string);
alter table external1 set tblproperties ('EXTERNAL'='TRUE');

-- truncate on external table should throw exception. Property value of 'EXTERNAL' is not case sensitive
truncate table external1;
