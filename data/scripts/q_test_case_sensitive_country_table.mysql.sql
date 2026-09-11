-- country and `Country` are distinct tables.
CREATE TABLE country
(
    id   int,
    name varchar(20)
);
insert into country values (1, 'India');
insert into country values (2, 'Russia');
insert into country values (3, 'USA');

CREATE TABLE `Country`
(
    id   int,
    name varchar(20)
);
insert into `Country` values (10, 'Italy');
insert into `Country` values (11, 'Greece');


