With raw as (Select date_trunc({{Period}},order_day) as Date, COUNT(distinct (ORDER_ID)) as total_orders,
Round(COUNT(distinct CASE WHEN PICKUP_VALID = 0 THEN order_id end) /total_orders *100,1) as Pickup_breach,
Round(COUNT(distinct CASE WHEN DROP_VALID = 0 THEN order_id end)/total_orders  *100,1) as Drop_breach,
Round(Count(distinct Case When WAITING_VALID = 0 then order_id end)/total_orders *100,1) as Waiting_breach,
Round(Count(distinct Case when CUSTOMER_FARE > ESTIMATED_FARE then order_id end)/total_orders *100,1) as fare_breach,
Round(Count(distinct Case when ESTIMATED_FARE <> 0 and (CUSTOMER_FARE / ESTIMATED_FARE)  >= 1.05 then order_id end)/total_orders *100,1) as "FARE_BREACH_+5PERC",
Round(Count(distinct Case when (CUSTOMER_FARE - ESTIMATED_FARE) >= 100 then order_id end)/total_orders *100,1) as "FARE_BREACH_+100Rs",


from prod_eldoria.mart.breach_pricing_observability A
left join trucks.vehicle_segment_mapping_v2 B
on A.vehicle_id = B.vehicle_id
left join trucks.geo_regions_roi g
on g.id = A.geo_region_id
WHERE 
ORDER_DAY >= {{Start_Date}}
and ORDER_DAY <= {{End_Date}}
and A.vehicle_id = {{vehicle_id}}
[[and A.geo_region_id in ({{geo_region_id}})]]
[[and g.tier_status = {{tier}}]]
group by 1)

SELECT 
    Date, 
    key AS Perticulars, 
    value 
FROM 
    raw
UNPIVOT(
    value FOR key IN (Pickup_breach,Drop_breach,Waiting_breach,fare_breach,"FARE_BREACH_+5PERC","FARE_BREACH_+100Rs"))
    order by 1 desc , 3 desc