select 
upper(OWNER_PAN) OWNER_PAN
,OWNER_NAME
,OWNER_PHONE
,lower(OWNER_EMAIL) OWNER_EMAIL
,VEHICLE_DRIVER
,DRIVER_ID
,DRIVER_DL_NUMBER
,upper(VEHICLE_REGISTRATION_NUMBER) RC_Number
,upper(VEHICLE_CHASSIS_NUMBER) CHASSIS_NUMBER -- for 1 to many RC & Chassis. 17digit
,VEHICLE_COMPANY
,VEHICLE_BODY_TYPE

from prod_curated.oms_public.driver_enrollment_details where upper(OWNER_PAN)='DJEPA5104F' ;


---- check mapping type RC X Driver Do 1 Rc has 1 driver or many ?
select upper(VEHICLE_REGISTRATION_NUMBER) RC_Number,
count(driver_id) drvrs, count( distinct driver_id) uniq,drvrs-uniq diff
from prod_curated.oms_public.driver_enrollment_details group by 1 order by 2 desc;