create table Part1 (PNum int, OrderOnHand int);
insert into Part1 values (3,6),(10,1),(8,0);
create table Supply (PNum int, Qty int);
insert into Supply values (3,4),(3,2),(10,1);


select pnum from Part1 p where OrderOnHand in
                (select count(*) from Supply s where s.pnum = p.pnum);
