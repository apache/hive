create table if not exists country (name varchar(255) not null);
create table if not exists state (name varchar(255) not null, country_code int not null);
create table if not exists city (name varchar(255) not null, state_code int not null);

insert into country (name) values ('India');
insert into country (name) values ('Russia');
insert into country (name) values ('USA');

insert into state (name,country_code) values ('Maharashtra',1);
insert into state (name,country_code) values ('Madhya Pradesh',1);
insert into state (name,country_code) values ('Moscow',3);
insert into state (name,country_code) values ('Something',4);
insert into state (name,country_code) values ('Florida',4);
insert into state (name,country_code) values ('Texas',4);

insert into city (name,state_code) values('Mumbai',1);
insert into city (name,state_code) values('Pune',1);
insert into city (name,state_code) values('Bhopal',2);
insert into city (name,state_code) values('Indore',2);
insert into city (name,state_code) values('Klin',3);
insert into city (name,state_code) values('Los Angeles',5);
insert into city (name,state_code) values('Plant City',5);
insert into city (name,state_code) values('Arlington',6);
