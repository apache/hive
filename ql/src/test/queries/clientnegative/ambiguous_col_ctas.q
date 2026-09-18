-- Cross-alias ambiguous reference in a CTAS: unchecked, one arbitrarily-chosen candidate is
-- silently persisted into the table. Keep this test even if the check ever exempts statements
-- Hive generates for itself (rewritten MERGE/UPDATE/DELETE, materialised CTEs): a user CTAS
-- must never be exempted.
create table ctas_ambiguous_ref as
  with bse as (select 'FIRST' as c, 'SECOND' as c),
       tpm as (select * from bse)
  select tpm.c from tpm;
