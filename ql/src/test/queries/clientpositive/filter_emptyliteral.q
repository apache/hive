create table employee(firstname varchar(255), lastname char(25), id int);

explain cbo SELECT id FROM employee where firstname = '';
explain cbo SELECT id FROM employee where lastname = '';
