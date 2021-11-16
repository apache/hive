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

-- Create a user and associate them with a default schema <=> search_path
CREATE ROLE greg WITH LOGIN PASSWORD 'GregPass123!$';
ALTER ROLE greg SET search_path TO bob;
-- Grant the necessary permissions to be able to access the schema
GRANT USAGE ON SCHEMA bob TO greg;
GRANT SELECT ON ALL TABLES IN SCHEMA bob TO greg;
