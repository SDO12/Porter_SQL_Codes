with doj as (Select d.id as driver_id,d.uuid, d.geo_region_id,vsm.level0_mapping, vsm.vehicle_category, date_trunc('day', date_of_join + interval '5 hours, 30 mins') as joining_month,
date_trunc('day',to_timestamp(waitlisted_at) + interval '5 hours, 30 mins') as waitlist_started_month,
date_trunc('day',to_timestamp(waitlist_ended_at) + interval '5 hours, 30 mins') as waitlist_ended_month from prod_curated.oms_public.drivers d
left join prod_curated.trucks.vehicle_segment_mapping_v2 vsm on d.vehicle_id = vsm.vehicle_id
left join prod_curated.oms_public.partner_onboarding_waitlistings pow on d.id = pow.role_id and pow.onboardee_role_type = 'DRIVER' 
where date_of_join + interval '5 hours, 30 mins' >= {{date_of_join}}
and is_test = 'False' and deleted_at is null and d.vehicle_id != 97),

M_orders as (Select doj.driver_id,doj.uuid,doj.geo_region_id,level0_mapping,vehicle_category, doj.joining_month,
waitlist_started_month,
waitlist_ended_month,
min(case when o.customer_mobile = '0000000001' and o.status = 4 then o.order_id else null end) as dummy_order_status,
doj.joining_month + interval '30 day' as M0_month,
count(distinct CASE WHEN o.created_at + interval '5 hours, 30 mins' BETWEEN doj.joining_month AND doj.joining_month + interval '30 days' and o.status = 4 and o.deleted_at is null and o.order_type = 0 then o.order_id else null end) as M0_orders,
count(distinct case WHEN o.created_at + interval '5 hours, 30 mins' BETWEEN doj.joining_month + interval '31 days' AND doj.joining_month + interval '60 days' and o.status = 4 and o.deleted_at is null and o.order_type = 0 then o.order_id else null end) as M1_orders,
count(distinct case WHEN o.created_at + interval '5 hours, 30 mins' BETWEEN doj.joining_month + interval '61 days' AND doj.joining_month + interval '90 days' and o.status = 4 and o.deleted_at is null and o.order_type = 0 then o.order_id else null end) as M2_orders,
count(distinct case WHEN o.created_at + interval '5 hours, 30 mins' BETWEEN doj.joining_month + interval '91 days' AND doj.joining_month + interval '120 days' and o.status = 4 and o.deleted_at is null and o.order_type = 0 then o.order_id else null end) as M3_orders,
count(distinct case WHEN o.created_at + interval '5 hours, 30 mins' BETWEEN doj.joining_month + interval '121 days' AND doj.joining_month + interval '150 days' and o.status = 4 and o.deleted_at is null and o.order_type = 0 then o.order_id else null end) as M4_orders,
count(distinct case WHEN o.created_at + interval '5 hours, 30 mins' BETWEEN doj.joining_month + interval '151 days' AND doj.joining_month + interval '180 days' and o.status = 4 and o.deleted_at is null and o.order_type = 0 then o.order_id else null end) as M5_orders,
count(distinct case WHEN o.created_at + interval '5 hours, 30 mins' BETWEEN doj.joining_month + interval '181 days' AND doj.joining_month + interval '210 days' and o.status = 4 and o.deleted_at is null and o.order_type = 0 then o.order_id else null end) as M6_orders,
from doj left join prod_curated.oms_public.orders o on doj.driver_id = o.driver_id group by 1,2,3,4,5,6,7,8,10),


monthly_active_partners AS (
        SELECT distinct di.driver_id, date_trunc('DAY',dld.login_date) as month, sum(dld.total_business_login_time) as total_business_login_time
        FROM prod_curated.trucks.driver_login_data dld 
        JOIN prod_curated.trucks.driver_info di ON dld.driver_id = di.driver_id 
        JOIN prod_curated.trucks.vehicle_segment_mapping_v2 vsm ON di.vehicle_id = vsm.vehicle_id 
        WHERE dld.login_date >= '2024-03-01'
        -- AND dld.total_business_login_time >= 1
        GROUP BY 1, 2
        having sum(dld.total_business_login_time) >=1
),

FINAL AS (Select date_trunc('month',date(joining_month)) as Joining_month,
M_orders.vehicle_category as Vehicle_category,
count(distinct M_orders.driver_id) as A0_Onboarded_drivers,
count(DISTINCT CASE 
           WHEN ((waitlist_ended_month <= joining_month + interval '30 days' OR (waitlist_ended_month IS NULL AND waitlist_started_month IS NULL)) 
                 AND dummy_order_status IS NOT NULL) 
           THEN M_orders.driver_id 
           ELSE NULL 
       END)  as A1_M0_eligible_drivers,
COUNT(DISTINCT CASE WHEN monthly_active_partners.driver_id IS NOT NULL THEN monthly_active_partners.driver_id ELSE NULL END) AS A2_Monthly_login_1hr,
count(DISTINCT CASE 
           WHEN ((waitlist_ended_month <= joining_month + interval '30 days' OR (waitlist_ended_month IS NULL AND waitlist_started_month IS NULL)) 
                 AND dummy_order_status IS NOT NULL)  and M0_orders > 0 then M_orders.driver_id else null end) as A3_M0_activated,
count(DISTINCT CASE 
           WHEN ((waitlist_ended_month <= joining_month + interval '30 days' OR (waitlist_ended_month IS NULL AND waitlist_started_month IS NULL)) 
                 AND dummy_order_status IS NOT NULL)  and M0_orders >= 3 then M_orders.driver_id else null end) as A4_M0_quality_activated,
count(DISTINCT CASE 
           WHEN ((waitlist_ended_month <= joining_month + interval '30 days' OR (waitlist_ended_month IS NULL AND waitlist_started_month IS NULL)) 
                 AND dummy_order_status IS NOT NULL) and M0_orders > 0 and M1_orders > 0 then M_orders.driver_id else null end) as A5_M1_retained,
count(distinct CASE 
           WHEN ((waitlist_ended_month <= joining_month + interval '30 days' OR (waitlist_ended_month IS NULL AND waitlist_started_month IS NULL)) 
                 AND dummy_order_status IS NOT NULL) and M0_orders > 0 and M1_orders > 0 and M2_orders > 0 then M_orders.driver_id else null end) as A6_M2_retained,
count(distinct CASE 
           WHEN ((waitlist_ended_month <= joining_month + interval '30 days' OR (waitlist_ended_month IS NULL AND waitlist_started_month IS NULL)) 
                 AND dummy_order_status IS NOT NULL) and M0_orders > 0 and M1_orders > 0 and M2_orders > 0 and M3_orders > 0 then M_orders.driver_id else null end) as A7_M3_retained,
count(distinct CASE 
           WHEN ((waitlist_ended_month <= joining_month + interval '30 days' OR (waitlist_ended_month IS NULL AND waitlist_started_month IS NULL)) 
                 AND dummy_order_status IS NOT NULL) and M0_orders > 0 and M1_orders > 0 and M2_orders > 0 and M3_orders > 0  and M4_orders > 0 then M_orders.driver_id else null end) as A8_M4_retained,
count(distinct CASE 
           WHEN ((waitlist_ended_month <= joining_month + interval '30 days' OR (waitlist_ended_month IS NULL AND waitlist_started_month IS NULL)) 
                 AND dummy_order_status IS NOT NULL) and M0_orders > 0 and M1_orders > 0 and M2_orders > 0 and M3_orders > 0  and M4_orders > 0  and M5_orders > 0 then M_orders.driver_id else null end) as A9_M5_retained,
count(distinct CASE 
           WHEN ((waitlist_ended_month <= joining_month + interval '30 days' OR (waitlist_ended_month IS NULL AND waitlist_started_month IS NULL)) 
                 AND dummy_order_status IS NOT NULL) and M0_orders > 0 and M1_orders > 0 and M2_orders > 0 and M3_orders > 0  and M4_orders > 0  and M5_orders > 0 and M6_orders > 0 then M_orders.driver_id else null end) as B0_M6_retained,


from M_orders
join prod_curated.redshift_analytics.vehicle_segment_mapping_v2 vsm on M_orders.vehicle_category = vsm.VEHICLE_CATEGORY
LEFT JOIN monthly_active_partners ON date_trunc('MONTH',M_orders.joining_month) = date_trunc('MONTH',monthly_active_partners.month)
and M_orders.driver_id = monthly_active_partners.driver_id
where vsm.level0_mapping in ('Micro LCV', 'LCV')
[[and M_orders.vehicle_category = {{Vehicle_category}}]]
[[and M_orders.geo_region_id in ({{geo_region_id}})]]
group by 1,2
order by 1,2
)


SELECT Joining_month,KEY AS  Metrics, Value
FROM FINAL
UNPIVOT(
    Value FOR KEY IN (
        A0_Onboarded_drivers,
        A1_M0_eligible_drivers,
        A2_Monthly_login_1hr,
        A3_M0_activated, A4_M0_quality_activated, A5_M1_retained, A6_M2_retained, A7_M3_retained, A8_M4_retained, A9_M5_retained, B0_M6_retained
    )
) AS unpvt
ORDER BY Joining_month DESC,2;