-- country (unquoted, folded to COUNTRY) and "Country" (quoted, case-sensitive) are distinct tables.
ALTER SESSION SET CONTAINER = XEPDB1;

CREATE USER bob IDENTIFIED BY bobpass;
ALTER USER bob QUOTA UNLIMITED ON users;
GRANT CREATE SESSION TO bob;

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


