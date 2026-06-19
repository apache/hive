
explain select 100.001BD;

explain select 100.000BD;

explain select 0.000BD;

explain select 0.100BD;

explain select 0.010BD;

explain select cast(0.010 as decimal(6,3));

explain select 0.09765625BD * 0.09765625BD * 0.0125BD * 578992BD;
select 0.09765625BD * 0.09765625BD * 0.0125BD * 578992BD;
