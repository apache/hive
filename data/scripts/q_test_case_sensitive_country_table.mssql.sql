-- A case-sensitive collation makes country and [Country] distinct tables.
CREATE DATABASE worldcs COLLATE Latin1_General_CS_AS;
USE worldcs;

CREATE SCHEMA bob;

CREATE TABLE bob.country
(
    id   int,
    name varchar(20)
);
insert into bob.country values (1, 'India');
insert into bob.country values (2, 'Russia');
insert into bob.country values (3, 'USA');

CREATE TABLE bob.[Country]
(
    id   int,
    name varchar(20)
);
insert into bob.[Country] values (10, 'Italy');
insert into bob.[Country] values (11, 'Greece');


