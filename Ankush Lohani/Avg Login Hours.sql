select login_period,
avg(login_hours) as avg_login_hours, avg(business_login_hours) as avg_business_login_hours
from(
select date_trunc({{period}}, dld.login_date) as login_period,
dld.driver_id,
dld.geo_region_id, 
vsm.vehicle_category,
level0_mapping,
sum(dld.total_business_login_time) as business_login_hours,
sum(dld.TOTAL_LOGIN_HOURS) as login_hours
from trucks.DRIVER_DAILY_PERFORMANCE_BUSINESS_HOURS DLD
join trucks.vehicle_segment_mapping_v2 vsm  on dld.vehicle_id=vsm.vehicle_id
LEFT join trucks.geo_regions_roi gr on dld.geo_region_id = gr.id
where login_date between {{start_date}} and {{end_date}}
[[and dld.vehicle_id in ({{vehicle_id}})]]
--[[and vsm.vehicle_category = {{vehicle_category}]]
[[and gr.tier_status = {{Tier}}]]
[[and dld.geo_region_id in ({{geo_region_id}})]]
group by 1,2,3,4,5
)
where true
group by 1
order by 1 desc;