-- In Oracle diving the tables in different namespaces/schemas is achieved via different users. The CREATE SCHEMA
-- statement exists in Oracle but has different semantics than those defined by SQL Standard and those adopted in other
-- DBMS.

-- In order to create the so-called "local" users in oracle you need to be connected to the Pluggable Database (PDB)
-- and not to the Container Database (CDB). In Oracle XE edition, used by this tests, the default and only PDB is
-- XEPDB1.
ALTER SESSION SET CONTAINER = XEPDB1;

-- Create the bob schema/user and give appropriate connections to be able to connect to the database
CREATE USER bob IDENTIFIED BY bobpass;
ALTER USER bob QUOTA UNLIMITED ON users;
GRANT CREATE SESSION TO bob;

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

-- Create the alice schema/user and give appropriate connections to be able to connect to the database
CREATE USER alice IDENTIFIED BY alicepass;
ALTER USER alice QUOTA UNLIMITED ON users;

GRANT CREATE SESSION TO alice;
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

-- Without the SELECT ANY privilege a user cannot see the tables/views of another user. In other words when a user
-- connects to the database using a specific user and schema it is not possible refer to tables in another user/schema
-- namespace.
GRANT SELECT ANY TABLE TO bob;
GRANT SELECT ANY TABLE TO alice;
-- Allow the users to perform inserts on any table/view in the database, and not only those present on their own schema
GRANT INSERT ANY TABLE TO bob;
GRANT INSERT ANY TABLE TO alice;