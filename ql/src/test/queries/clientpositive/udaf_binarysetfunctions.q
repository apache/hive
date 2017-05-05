drop table t;
create table t (id int,px int,y decimal,x decimal);

insert into t values (101,1,1,1);
insert into t values (201,2,1,1);
insert into t values (301,3,1,1);
insert into t values (401,4,1,11);
insert into t values (501,5,1,null);
insert into t values (601,6,null,1);
insert into t values (701,6,null,null);
insert into t values (102,1,2,2);
insert into t values (202,2,1,2);
insert into t values (302,3,2,1);
insert into t values (402,4,2,12);
insert into t values (502,5,2,null);
insert into t values (602,6,null,2);
insert into t values (702,6,null,null);
insert into t values (103,1,3,3);
insert into t values (203,2,1,3);
insert into t values (303,3,3,1);
insert into t values (403,4,3,13);
insert into t values (503,5,3,null);
insert into t values (603,6,null,3);
insert into t values (703,6,null,null);
insert into t values (104,1,4,4);
insert into t values (204,2,1,4);
insert into t values (304,3,4,1);
insert into t values (404,4,4,14);
insert into t values (504,5,4,null);
insert into t values (604,6,null,4);
insert into t values (704,6,null,null);
insert into t values (800,7,1,1);


explain select px,var_pop(x),var_pop(y),corr(y,x),covar_samp(y,x),covar_pop(y,x),regr_count(y,x),regr_slope(y,x),
regr_intercept(y,x), regr_r2(y,x), regr_sxx(y,x), regr_syy(y,x), regr_sxy(y,x), regr_avgx(y,x), regr_avgy(y,x), regr_count(y,x)
 from t group by px order by px;

select	px,
	round(	var_pop(x),5),
	round(	var_pop(y),5),
	round(	corr(y,x),5),
	round(	covar_samp(y,x),5),
	round(	covar_pop(y,x),5),
	regr_count(y,x),
	round(	regr_slope(y,x),5),
	round(	regr_intercept(y,x),5),
	round(	regr_r2(y,x),5),
	round(	regr_sxx(y,x),5),
	round(	regr_syy(y,x),5),
	round(	regr_sxy(y,x),5),
	round(	regr_avgx(y,x),5),
	round(	regr_avgy(y,x),5),
	round(	regr_count(y,x),5)
 from t group by px order by px;


select id,regr_count(y,x) over (partition by px) from t order by id;
