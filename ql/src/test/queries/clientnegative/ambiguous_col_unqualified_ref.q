-- The inner CTE references the duplicated alias by an UNQUALIFIED name, which is resolved by
-- TypeCheckProcFactory.ColumnExprProcessor (the "It's a column" branch) rather than by
-- processQualifiedColRef. Every other ambiguity test uses a qualified reference, so this is the
-- only coverage of that check site. The error names the definition-site alias (bse), matching
-- what the non-CBO path reports for the same query.
with bse as (select 'a' as c, 'b' as c),
     tpm as (select c from bse)
select tpm.c from tpm;
