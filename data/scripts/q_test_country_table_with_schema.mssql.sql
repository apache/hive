CREATE DATABASE world;
USE world;

CREATE SCHEMA bob;
CREATE TABLE bob.country
(
    id   int,
    name varchar(20)
);

insert into bob.country
values (1, 'India');
insert into bob.country
values (2, 'Russia');
insert into bob.country
values (3, 'USA');

CREATE SCHEMA alice;
CREATE TABLE alice.country
(
    id   int,
    name varchar(20)
);

insert into alice.country
values (4, 'Italy');
insert into alice.country
values (5, 'Greece');
insert into alice.country
values (6, 'China');
insert into alice.country
values (7, 'Japan');

-- Create a user and associate them with a default schema
CREATE LOGIN greg WITH PASSWORD = 'GregPass123!$';
CREATE USER greg FOR LOGIN greg WITH DEFAULT_SCHEMA=bob;
-- Allow the user to connect to the database and run queries
GRANT CONNECT, SELECT TO greg;
