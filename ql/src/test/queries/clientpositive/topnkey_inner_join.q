drop table if exists customer;
drop table if exists orders;

create table customer (id int, name string, email string);
create table orders (customer_id int , amount int);

alter table customer add constraint pk_customer_id primary key (id) disable novalidate rely;
alter table orders add constraint fk_order_customer_id foreign key (customer_id) references customer(id) disable novalidate rely;

insert into customer values
  (4, 'Heisenberg', 'heisenberg@email.com'),
  (3, 'Smith', 'smith@email.com'),
  (2, 'Jones', 'jones@email.com'),
  (1, 'Robinson', 'robinson@email.com');

insert into orders values
  (null, 1),
  (2, 200),
  (3, 40),
  (1, 100),
  (1, 50),
  (3, 30);

set hive.optimize.topnkey=true;
set hive.optimize.limittranspose=false;

select 'positive: order by columns are coming from child table';
explain select * from orders join customer on customer.id = orders.customer_id order by orders.customer_id limit 3;
explain select * from orders join customer on customer.id = orders.customer_id order by orders.customer_id, orders.amount limit 3;
explain select * from customer join orders on orders.customer_id = customer.id order by orders.amount, orders.customer_id limit 3;
select * from orders join customer on customer.id = orders.customer_id order by orders.customer_id limit 3;
select * from orders join customer on customer.id = orders.customer_id order by orders.customer_id, orders.amount limit 3;
select * from customer join orders on orders.customer_id = customer.id order by orders.amount, orders.customer_id limit 3;

select 'negative: order by columns are coming from referenced table';
explain select * from orders join customer on customer.id = orders.customer_id order by customer.name limit 3;
explain select * from orders join customer on customer.id = orders.customer_id order by customer.email, customer.name limit 3;
select * from orders join customer on customer.id = orders.customer_id order by customer.name limit 3;
select * from orders join customer on customer.id = orders.customer_id order by customer.email, customer.name limit 3;

select 'negative: 1st order by columns is coming from referenced table';
explain select * from orders join customer on customer.id = orders.customer_id order by customer.name, orders.amount limit 3;
select * from orders join customer on customer.id = orders.customer_id order by customer.name, orders.amount limit 3;

select 'mixed/positive: 1st n order by columns are coming from child table';
explain select * from orders join customer on customer.id = orders.customer_id order by orders.customer_id, customer.name limit 3;
select * from orders join customer on customer.id = orders.customer_id order by orders.customer_id, customer.name limit 3;

select 'negative: nulls first';
explain select * from customer join orders on orders.customer_id = customer.id order by customer_id nulls first limit 1;
select * from customer join orders on orders.customer_id = customer.id order by customer_id nulls first limit 1;

select 'negative: no PK/FK';
alter table customer drop constraint pk_customer_id;
alter table orders drop constraint fk_order_customer_id;
explain select * from customer join orders on customer.id = orders.customer_id order by customer.id limit 3;
select * from customer join orders on customer.id = orders.customer_id order by customer.id limit 3;

select 'negatie: no RELY';
alter table customer add constraint pk_customer_id primary key (id) disable novalidate;
alter table orders add constraint fk_order_customer_id foreign key (customer_id) references customer(id) disable novalidate;
explain select * from customer join orders on customer.id = orders.customer_id order by customer.id limit 3;
select * from customer join orders on customer.id = orders.customer_id order by customer.id limit 3;
