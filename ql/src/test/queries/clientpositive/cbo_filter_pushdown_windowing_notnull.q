-- HIVE-29729: an IS NOT NULL predicate above nested windowing (OVER) Projects must
-- not trip the RedundancyChecker in HiveFilterProjectTransposeRule.
set hive.cbo.fallback.strategy=NEVER;

create table cl_wl_daily_table (
  user_id string,
  lender string,
  attributes string,
  loan_type string,
  whitelisting_status string
) stored as orc;

explain
select
  user_id,
  cg_tg_cohort
from (
  select
    user_id,
    lender,
    maximum_principal,
    loan_type,
    case
      when loan_type = 'FRESH' and rnk <= (333333 / user_count) * 0.1 then 'CG'
      when loan_type = 'FRESH' and rnk <= (333333 / user_count) then 'TG'
      when loan_type = 'REPEAT' and rnk <= (500 / user_count)
           and substring(user_id, -2) between '60' and '69' then 'CG'
      when loan_type = 'REPEAT' and rnk <= (500 / user_count)
           and substring(user_id, -2) between '00' and '45' then 'TG'
      when loan_type = 'REPEAT' and rnk <= (500 / user_count) then 'TG-TELE'
    end as cg_tg_cohort
  from (
    select
      user_id,
      lender,
      maximum_principal,
      loan_type,
      percent_rank() over(partition by loan_type order by rnk) as rnk,
      count(user_id) over(partition by loan_type) as user_count
    from (
      select
        user_id,
        lender,
        maximum_principal,
        loan_type,
        row_number() over(partition by loan_type order by rand()) as rnk
      from (
        select
          wl.user_id as user_id,
          max(lender) as lender,
          max(maximum_principal) as maximum_principal,
          max(loan_type) as loan_type
        from (
          select
            user_id,
            lender,
            case
              when repeat_wl = 0 and platform_repeat_wl = 0 then 'FRESH'
              else 'REPEAT'
            end as loan_type,
            cast(maximum_principal / 100 as int) as maximum_principal
          from (
            select
              user_id,
              concat_ws('#', sort_array(collect_set(lender))) as lender,
              max(
                case
                  when coalesce(get_json_object(attributes, '$.maximum_principal'), '_') = '_' then 0
                  else get_json_object(attributes, '$.maximum_principal')
                end
              ) as maximum_principal,
              max(case when loan_type = 'REPEAT' then 1 else 0 end) as repeat_wl,
              max(case when loan_type = 'PLATFORM_REPEAT' then 1 else 0 end) as platform_repeat_wl
            from cl_wl_daily_table
            where whitelisting_status = 'Currently WL'
            group by user_id
          ) base
          where maximum_principal > 0
            and maximum_principal is not null
            and maximum_principal <> '_'
        ) wl
        group by
          wl.user_id
      ) grouped_users
    ) ranked_once
  ) ranked_twice
) cohorted
where cg_tg_cohort is not null;
