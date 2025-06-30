select date_trunc({{period}}, order_date) as order_period, 
sum(demand) as overall_demand, 
sum(unique_demand) as unique_demands, 
div0(unique_demands,overall_demand) as perc_unique_demand, 
sum(completed_orders) as completed_orders,
div0(sum(completed_orders),unique_demands) as unique_fulfillment,
div0(sum(completed_orders),overall_demand) as overall_fulfillment,
div0(sum(orders_allocated),sum(unique_demand)) as allocation_perc,
div0(sum(MISSED_ORDER),sum(unique_demand)) as missed_order_perc,
div0(sum(stockout),sum(unique_demand)) as stockout_perc
from prod_curated.trucks.trucks_unique_demand_summary tuds
LEFT join trucks.geo_regions_roi gr on tuds.geo_region_id = gr.id
where tuds.order_date between {{start_date}} and {{end_date}}
and tuds.vehicle_id = {{VEHICLE_ID}} 
[[and geo_region_id in ({{geo_region_id}})]]
[[and gr.tier_status = {{Tier}}]]
group by 1
order by 1 desc