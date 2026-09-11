-- country (unquoted, lowercase) and "Country" (quoted, case-sensitive) are distinct tables.
CREATE SCHEMA bob;

CREATE TABLE bob.country
(
    id   int,
    name varchar(20)
);
insert into bob.country values (1, 'India');
insert into bob.country values (2, 'Russia');
insert into bob.country values (3, 'USA');

CREATE TABLE bob."Country"
(
    id   int,
    name varchar(20)
);
insert into bob."Country" values (10, 'Italy');
insert into bob."Country" values (11, 'Greece');


