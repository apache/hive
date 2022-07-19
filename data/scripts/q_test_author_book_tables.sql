create table author
(
    id int,
    fname       varchar(20),
    lname       varchar(20)
);
insert into author values (1, 'Victor', 'Hugo');
insert into author values (2, 'Alexandre', 'Dumas');

create table book
(
    id     int,
    title  varchar(100),
    author int
);
insert into book
values (1, 'Les Miserables', 1);
insert into book
values (2, 'The Count Of Monte Cristo', 2);
