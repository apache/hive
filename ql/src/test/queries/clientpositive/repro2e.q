explain
create table if not exists chejianer_temp.hhq_enquiry_offer_statics2 as
select 
enquiry_type kh_after_sku_cnt
from 
(
    select
    d.vendor_company_id,
    d.vendor_company_name,
    d.enquiry_type,
    d.um_code,
    d.third_institution,
    d.assess_um_code,
    d.enquiry_intention_type,
    d.enquiry_intention_flag,
    count
    (
        distinct d.enquiry_allocation_id
    ) enquiry_cnt,
    count
    (
        distinct  d.enquiry_allocation_id+1
    ) enquiry_cnt_pc_lq,
    count
    (
        distinct d.enquiry_allocation_id +1
    ) enquiry_cnt_app_lq,
    count
    (
        distinct d.enquiry_allocation_id +11
    ) enquiry_cnt_im_lq,
    count
    (
        distinct case when d.enquiry_offer_type_0610 <>'未报价' then d.enquiry_allocation_id
        end
    ) offer_cnt,
    count
    (
        distinct case when d.enquiry_offer_type_0610 <>'未报价'
         then d.enquiry_allocation_id end
    ) offer_cnt_pc_bj,
    count
    (
        distinct case when d.enquiry_offer_type_0610 <>'未报价'
        then d.enquiry_allocation_id end
    ) offer_cnt_app_bj,
    count
    (
        distinct case when d.enquiry_offer_type_0610 <>'未报价'
        then d.enquiry_allocation_id end
    ) offer_cnt_im_bj,
    count
    (
        distinct case when d.reply_timediff>= 0
        and d.reply_timediff <= 10 then d.enquiry_allocation_id end
    ) offer_cnt_min_10,
    count
    (
        distinct case when d.reply_timediff>= 0
        and d.reply_timediff <= 20 then d.enquiry_allocation_id end
    ) offer_cnt_min_20,
    count
    (
        distinct case when d.reply_timediff>= 0
        and d.reply_timediff <= 30 then d.enquiry_allocation_id end
    ) offer_cnt_min_30,
    count
    (
        distinct case when d.reply_timediff>= 0
        and d.reply_timediff <= 60 then d.enquiry_allocation_id end
    ) offer_cnt_min_60,
    count
    (
        distinct case when d.enquiry_offer_flag = '超时报价' then d.enquiry_allocation_id
        end
    ) offer_over_cnt,
    count
    (
        distinct case when d.enquiry_offer_flag = '未报价' then d.enquiry_allocation_id
        end
    ) empty_offer_cnt,
    count
    (
        distinct case when d.enquiry_offer_flag = '无货报价'
        and d.empty_offer_parts_cnt = d.enquiry_parts_num then d.enquiry_allocation_id
        end
    ) all_empty_offer_cnt,
    count
    (
        distinct d.part_enquiry_id
    ) enquiry_total_parts_num,
    count
    (
        distinct case when d.part_price>0 then d.part_enquiry_id end
    ) enquiry_offer_price_parts_num,
    count
    (
        distinct case when d.part_price>0 then d.part_enquiry_id end
    ) / count
    (
        distinct d.part_enquiry_id
    ) enquiry_offer_price_parts_ratio,
    count
    (
        distinct case when d.part_price>0 then d.part_enquiry_id end
    ) /
    count
    (
        distinct case when d.enquiry_offer_type_0610 <>'未报价' then d.part_enquiry_id
        end
    ) enquiry_offer_price_parts_ratio_offer,
    count
    (
        distinct case when d.enquiry_type = '直供'
        and d.desc_rk = 1
        and d.part_offer_status = '1' then d.part_enquiry_id end
    ) offer_no_parts_num,
    count
    (
        distinct case when d.ea_status = '4' then d.enquiry_allocation_id end
    ) total_empty_offer_cnt,
    count
    (
        distinct case when d.ea_status = '4' then d.part_enquiry_id end
    ) total_offer_no_parts_num,
    count
    (
        distinct case when d.response_time>= 0
        and d.response_time <= 5 then d.enquiry_allocation_id end
    ) response_cnt_min_5,
    count
    (
        distinct case when d.response_time>= 0
        and d.response_time <= 10 then d.enquiry_allocation_id end
    ) response_cnt_min_10,
    count
    (
        distinct case when d.response_time>= 0
        and d.response_time <= 20 then d.enquiry_allocation_id end
    ) response_cnt_min_20,
    count
    (
        distinct case when d.response_time>= 0
        and d.response_time <= 30 then d.enquiry_allocation_id end
    ) response_cnt_min_30,
    d.response_time
    from chejianer_temp.hhq_enquiry_offer_parts_detail d
    group by
    d.vendor_company_id,
    d.vendor_company_id*d.vendor_company_id,
    d.vendor_company_name,
    d.enquiry_type,
    d.um_code,
    d.third_institution,
    d.vendor_charge_area,
    d.vendor_charge_person,
    d.vendor_second_institution,
    d.vendor_company_city,
    d.vendor_company_county,
    d.true_vendor_company,
    d.vendor_company_type,
    d.vehicle_flag,
    d.is_direct_vendor_company,
    d.enquiry_source,
    d.enquiry_trace_source,
    d.enquiry_car_brand,
    d.car_brand_type,
    d.buyer_enquiry_id,
    d.buyer_enquiry_id*    d.buyer_enquiry_id,
    d.enquiry_id,
    d.vin_code,
    d.not_bidding,
    d.vendor_account_id,
    d.vendor_account_name,
    d.vendor_account_role,
    d.buyer_company_id,
    d.buyer_company_name,
    d.true_company,
    d.buyer_charge_area,
    d.buyer_charge_person,
    d.buyer_second_institution,
    d.buyer_company_city,
    d.buyer_company_county,
    d.reply_type,
    d.buyer_third_institution,
    d.buyer_charge_zone,
    d.buyer_charge_zone_person,
    d.assess_um_code,
    d.enquiry_intention_type,
    d.enquiry_intention_flag,
    d.response_time,
    d.enquiry_allocation_id*d.enquiry_allocation_id
) a
;