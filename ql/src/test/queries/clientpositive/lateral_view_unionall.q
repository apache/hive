create table tez_test_t1(md_exper string);
insert into tez_test_t1 values('tez_test_t1-md_expr');

create table tez_test_t5(md_exper string, did string);
insert into tez_test_t5 values('tez_test_t5-md_expr','tez_test_t5-did');

create table tez_test_t2(did string);
insert into tez_test_t2 values('tez_test_t2-did');

explain
SELECT NULL AS first_login_did
   FROM tez_test_t5
   LATERAL VIEW explode(split('0,6', ',')) gaps AS ads_h5_gap
UNION ALL
SELECT  null as first_login_did
    FROM tez_test_t1
UNION ALL
   SELECT did AS first_login_did
   FROM tez_test_t2;

SELECT NULL AS first_login_did
   FROM tez_test_t5
   LATERAL VIEW explode(split('0,6', ',')) gaps AS ads_h5_gap
UNION ALL
SELECT  null as first_login_did
    FROM tez_test_t1
UNION ALL
   SELECT did AS first_login_did
   FROM tez_test_t2;
