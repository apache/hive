-- The lateral view reuses the table alias, so the exploded column collides with the base table
-- column under the same table alias. This is caught by the reference-time check in
-- RowResolver.get (ambiguousColumns), not by the duplicate-alias marker: the message form
-- "Ambiguous column reference: t.c" identifies that path. Keep this test: it is the only
-- coverage of that check, which would otherwise look like dead code and get removed.
create table lv_dup_alias (c int, arr array<int>);
select t.c from lv_dup_alias t lateral view explode(t.arr) t as c;
