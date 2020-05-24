
-- 10 torpedos carried by every ship
-- 10 types of ships
-- 10 admiral has 1 from each ship_type

drop table if exists admirals;
drop table if exists ship_types;
drop table if exists ships;
drop table if exists torpedos;

create table ships (id integer,ship_type_id integer,crew_size integer);
create table ship_types (id integer,type_name string);
insert into ship_types values
    (1,'galaxy class'),
    (2,'nebula class'),
    (3,'orion class'),
    (4,'first class'),
    (5,'last pass'),
    (6,'last pass'),
    (7,'akira class'),
    (8,'aeon type'),
    (9,'antares type'),
    (10,'apollo class')
    ;

create table admirals as
    select id from ship_types;

create table torpedos (id integer,ship_id integer,admiral_id integer);

insert into ships
select row_number() over (),t.id,row_number() over (partition by t.id) from ship_types t join ship_types t2;

insert into torpedos
select row_number() over (),s.id,row_number() over (partition by s.id) from ships s join ship_types t2;

