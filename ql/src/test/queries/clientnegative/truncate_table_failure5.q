create table external1 (col1 int, col2 string);
alter table external1 set tblproperties ('EXTERNAL'='true');

-- truncate on a non-managed table should throw exception
truncate table external1;
