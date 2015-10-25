explain
select srcpart.key, src1.value from
((srcpart inner join src on srcpart.key = src.key))
inner join src src1 on src1.value =srcpart.value;

explain
select srcpart.key, src1.value from
(srcpart inner join src on srcpart.key = src.key)
inner join src src1 on src1.value =srcpart.value;

explain
select srcpart.key, src1.value from
((srcpart inner join src on srcpart.key = src.key)
inner join src src1 on src1.value =srcpart.value);

explain
select srcpart.key, src1.value from
((srcpart inner join src on srcpart.key = src.key)
inner join src src1 on src1.value =srcpart.value)
inner join src src2 on src2.key = src1.key;
