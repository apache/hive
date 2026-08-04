create table cj1 (k int, v int);
create table cj2 (k int, w int);
create table ctas_dup_join as select a.k, b.k from cj1 a join cj2 b on a.k = b.k;
