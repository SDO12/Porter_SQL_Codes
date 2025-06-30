with test as(
select date_trunc({{period}},DATE(order_created_at)) period, 
count(order_id)::float as "A. demand",
count(case when allocated_driver_id is not null then order_id else null end)::float as "B. allocated_orders",
div0("B. allocated_orders","A. demand")*100 as "C. allocation_perc %",
div0(count(case when batch_bucket='accepted in 1st batch' then order_id else null end),"B. allocated_orders")*100 as "D. accepted_in_1st_batch %",
div0(count(case when batch_bucket='accepted in 2nd batch' then order_id else null end),"B. allocated_orders")*100
as "E. accepted_in_2nd_batch %",
div0(count(case when batch_bucket='accepted in 3rd batch' then order_id else null end),"B. allocated_orders")*100
as "F. accepted_in_3rd_batch %",
div0(count(case when batch_bucket='accepted post 3rd batch' then order_id else null end),"B. allocated_orders")*100
as "G. accepted_post_3rd_batch %",
div0(count(case when status=4 then order_id else null end),"A. demand")*100 as "H. fulfillment_perc %",
div0(count(case when batches_created=0 and allocated_driver_id is null and status=5 then order_id else null end),"A. demand")*100 as "I. stockout %",
div0(count(case when batches_created in (0,1) and DRIVERS_ADDED_TO_BATCH < 3 and status=5 then order_id else null end),"A. demand")*100 as "I1. new_stockout1 %",
div0(count(case when batches_created in (0,1) and DRIVERS_ADDED_TO_BATCH < 2 and status=5 then order_id else null end),"A. demand")*100 as "I2. new_stockout2 %",
div0(count(case when batches_created>0 and allocated_driver_id is null and status=5 then order_id else null end),"A. demand")*100 as "J. missed_order %",
div0(count(case when batches_created>0 and allocated_driver_id is null and status=5 and drivers_notified=0 then order_id else null end),"A. demand")*100 as "K. missed_order_due_to_notification_undelivery %"
from trucks.order_batching_info obi
join trucks.vehicle_segment_mapping_v2 vsm 
on obi.vehicle_id=vsm.vehicle_id
where date(order_created_at) between {{start_date}} and {{end_date}}
[[and geo_region_id={{geo_region_id}}]]
[[and obi.vehicle_id={{vehicle_id}}]]
[[and vsm.VEHICLE_CATEGORY={{vehicle_category}}]]
[[and (case when geo_region_id in (1,2,3,4,5,6,8,9) then 'Tier 1' else 'Tier 2' end)={{city_category}}]]
group by 1
),

unpivoted_data AS (
    SELECT 
        PERIOD,
        metric,
        value
    FROM test
    UNPIVOT (
        value FOR metric IN (
             "A. demand",
        "B. allocated_orders",
        "C. allocation_perc %",
        "D. accepted_in_1st_batch %",
        "E. accepted_in_2nd_batch %",
        "F. accepted_in_3rd_batch %",
        "G. accepted_post_3rd_batch %",
        "H. fulfillment_perc %",
        "I. stockout %", "I1. new_stockout1 %", "I2. new_stockout2 %",
        "J. missed_order %",
        "K. missed_order_due_to_notification_undelivery %"
        )
    )
)

select * from unpivoted_data
order by period desc, 
metric asc
;