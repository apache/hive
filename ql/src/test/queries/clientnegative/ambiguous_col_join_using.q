select t.c from (select * from (select 'a' as c, 'b' as c) s join (select 'a' as c) u using (c)) t;
