with bse as (select 'a' as c, 'b' as c),
     tpm as (select * from bse)
select tpm.c from tpm;
