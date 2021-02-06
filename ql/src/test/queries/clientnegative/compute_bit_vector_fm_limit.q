create table tab_int(a int);

-- insert some data
LOAD DATA LOCAL INPATH "../../data/files/int.txt" INTO TABLE tab_int;

-- this should raise an error since the number of bit vectors has a hard limit at 1024
select compute_bit_vector_fm(a, 10000) from tab_int;
