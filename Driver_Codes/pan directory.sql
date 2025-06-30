

select id driver_id,substr(owner_gstin,3,10) owner_pan , partner_owner_id owner_id
from prod_curated.oms_public.drivers

union all



select driver_id,owner_pan, from prod_curated.oms_public.driver_enrollment_details limit 10;