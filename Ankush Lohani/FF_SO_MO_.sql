Select Date_trunc({{Period}},ORDER_CREATED_DATE) Date, 
Round((Sum(Demand)-Sum(CBDF))/ Sum(Demand)*100,1) as Allocated,
Round(Sum(completed_orders) /Sum(Demand)*100,1) as Fulfilment,
Round(Sum(Missed_order)/ Sum(Demand)*100,1) as Missed_orders,
Round(Sum(STOCKOUT)/ Sum(Demand)*100,1) as STOCKOUT
from trucks.TRUCKS_DAILY_DEMAND_SUMMARY a
LEFT join trucks.geo_regions_roi gr
on a.geo_region_id = gr.id
where 
ORDER_CREATED_DATE >= {{start_date}}
and ORDER_CREATED_DATE <= {{end_date}}
[[and geo_region_id in ({{geo_region_id}})]]
[[and gr.tier_status = {{Tier}}]]
and vehicle_id = {{vehicle_id}}
group by 1
order by 1 desc