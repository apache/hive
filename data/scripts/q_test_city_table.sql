create table if not exists city
(
    name       varchar(255) not null,
    state int          not null
);

insert into city (name, state)
values ('Mumbai', 1);
insert into city (name, state)
values ('Pune', 1);
insert into city (name, state)
values ('Bhopal', 2);
insert into city (name, state)
values ('Indore', 2);
insert into city (name, state)
values ('Klin', 3);
insert into city (name, state)
values ('Los Angeles', 5);
insert into city (name, state)
values ('Plant City', 5);
insert into city (name, state)
values ('Arlington', 6);
