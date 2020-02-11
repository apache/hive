
create table t (a string,vc varchar(10),c char(10));
insert into t values('bee','bee','bee'),('xxx','xxx','xxx');

select	assert_true(t0.v = t1.v) from
	(select hash(a) as v from t where a='bee') as t0
join	(select hash(a) as v from t where a='bee' or a='xbee') as t1 on (true);

select	assert_true(t0.v = t1.v) from
	(select hash(vc) as v from t where vc='bee') as t0
join	(select hash(vc) as v from t where vc='bee' or vc='xbee') as t1 on (true);

select	assert_true(t0.v = t1.v) from
	(select hash(c) as v from t where c='bee') as t0
join	(select hash(c) as v from t where c='bee' or c='xbee') as t1 on (true);


