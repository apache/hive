set hive.cbo.enable=false;
with bse as (select 'a' as delivery_date, concat('a') as delivery_date),
     tpm as (select * from bse)
select tpm.delivery_date from tpm;
