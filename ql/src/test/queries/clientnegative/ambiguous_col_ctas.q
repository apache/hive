-- A CTAS whose SELECT contains a cross-alias ambiguous reference. Without the ambiguity check
-- this silently persists one arbitrarily-chosen candidate into a table ('FIRST', discarding
-- 'SECOND') and reports nothing, so every downstream reader treats the arbitrary choice as fact.
-- Keep this test if the check ever gains an exemption for statements Hive generates for itself
-- (rewritten MERGE/UPDATE/DELETE, materialised CTEs): a USER CTAS must never be exempted.
create table ctas_ambiguous_ref as
  with bse as (select 'FIRST' as c, 'SECOND' as c),
       tpm as (select * from bse)
  select tpm.c from tpm;
