set hive.optimize.topnkey=true;

drop table if exists customer;
drop table if exists customer_address;

create table customer(id bigint, address_id bigint, name char(20));
create table address(id bigint, city varchar(60));

alter table address add constraint pk_ca1 primary key (id) disable novalidate rely;
alter table customer add constraint fk_ca1 foreign key (address_id) references address (id) disable novalidate rely;

insert into address values
  (1, 'London'),
  (2, 'Washington'),
  (3, 'New York'),
  (4, 'Hopewell');

insert into customer values
  (1, 1, 'Jon'),
  (2, 2, 'Peter'),
  (3, 3, 'Smith'),
  (4, 4, 'Joe'),
  (5, 4, 'Robert'),
  (6, 4, 'Heisenberg');

select 'negative: filter on the PK side';

explain select name, city
  from customer join address
  on customer.address_id = address.id
  and city = 'Hopewell'
 order by customer.id
 limit 3;

select name, city
  from customer join address
  on customer.address_id = address.id
  and city = 'Hopewell'
 order by customer.id
 limit 3;

select 'positive: filter on the FK side';

explain select name, city
   from customer join address
   on customer.address_id = address.id
   and name in ('Joe', 'Robert','Heisenberg')
  order by customer.address_id
  limit 3;

select name, city
   from customer join address
   on customer.address_id = address.id
   and name in ('Joe', 'Robert','Heisenberg')
  order by customer.address_id
  limit 3;
