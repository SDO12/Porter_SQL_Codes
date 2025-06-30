with 
enroll_data as (    
select distinct
upper(VEHICLE_REGISTRATION_NUMBER) RC_Number
,upper(OWNER_PAN) OWNER_PAN
,OWNER_NAME
,OWNER_PHONE
,lower(OWNER_EMAIL) OWNER_EMAIL
,VEHICLE_DRIVER
,DRIVER_ID
,DRIVER_DL_NUMBER
,upper(TRANSLATE(VEHICLE_CHASSIS_NUMBER, ' -_', '')) CHASSIS_NUMBER -- for 1 to many RC & Chassis. 17digit
,substr(CHASSIS_NUMBER,1,3) as WMI_code
,VEHICLE_COMPANY
,VEHICLE_BODY_TYPE
,RC_Number||'-'||DRIVER_ID as Driver_RC_key

from prod_curated.oms_public.driver_enrollment_details e
where upper(VEHICLE_REGISTRATION_NUMBER) is not null 
)
,
pan_base as (
 select distinct id driver_id
,DRIVER_NAME
,DATE_OF_JOIN as	Driver_DATE_OF_JOIN
,vehicle_number rc_number
,partner_owner_id
,owner_gstin
,driver_mobile
,secondary_driver_mobile
,STATUS	as	Driver_STATUS
,LOGIN_STATUS	as	Driver_LOGIN_STATUS
,GEO_REGION_ID	as	Driver_GEO_REGION_ID
,ACTIVE_STATUS	as	Driver_ACTIVE_STATUS
,SUSPEND_STATUS	as	Driver_SUSPEND_STATUS
,IS_TEST	as	Driver_IS_TEST
,EMAIL	as	Driver_email
,UUID	as	Driver_uuid
,substr(owner_gstin,3,10) PAN_of_GSTIN
,row_number() over (partition by PAN_of_GSTIN order by driver_id desc) RN

from prod_curated.oms_public.drivers
where owner_gstin is not null and PAN_of_GSTIN='DJEPA5104F'
 )



select
 w.Driver_RC_key
,w.driver_id
,w.DRIVER_NAME
,w.Driver_DATE_OF_JOIN		
,w.DRIVER_MOBILE		
,w.SECONDARY_DRIVER_MOBILE		
,w.RC_NUMBER		
,w.Driver_STATUS		
,w.Driver_LOGIN_STATUS		
,w.Driver_GEO_REGION_ID		
,w.Driver_ACTIVE_STATUS		
,w.Driver_SUSPEND_STATUS		
,w.Driver_IS_TEST		
,w.Driver_email		
,w.Driver_uuid		
,w.PARTNER_OWNER_ID	as 	Associate_Owner_id
,w.OWNER_GSTIN	as 	Associate_Owner_GSTN
,w.PAN_of_GSTIN	
,w.RN	as	driver_slno
,w.OWNER_CREATED_AT		
,w.FIRST_OWNER_SL		
,w.OWNER_FULL_NAME		
,w.OWNER_MOBILE		
,w.OWNER_EMAIL		
,w.OWNER_UUID		
,w.VENDOR_CODE		
,w.OWNER_TYPE		
,w.OWNER_GEO_REGION_ID		
,w.PRIMARY_OWNER_ID	as	Primary_Owner_id
,z.RC_Number	as	Enroll_RC_Number
,z.OWNER_PAN	as	Enroll_OWNER_PAN
,z.OWNER_NAME	as	Enroll_OWNER_NAME
,z.OWNER_PHONE	as	Enroll_OWNER_PHONE
,z.OWNER_EMAIL	as	Enroll_OWNER_EMAIL
,z.VEHICLE_DRIVER	as	Enroll_VEHICLE_DRIVER
,z.CHASSIS_NUMBER	as	Enroll_CHASSIS_NUMBER
,z.VEHICLE_COMPANY	as	Enroll_VEHICLE_COMPANY
,z.VEHICLE_BODY_TYPE	as	Enroll_VEHICLE_BODY_TYPE
,z.WMI_code
,MANUFACTURER
,COUNTRY as MANUFACTURing_COUNTRY
,w.PAN_of_GSTIN=Enroll_OWNER_PAN as is_PAN_match
,w.OWNER_MOBILE=Enroll_OWNER_PHONE as is_OWNER_MOBILE_match
,w.OWNER_EMAIL=Enroll_OWNER_EMAIL as is_OWNER_EMAIL_match

from (
select p.*  , FIRST_VALUE(primary_owner_find) over ( partition by PAN_of_GSTIN order by first_owner_sl asc ) primary_owner_id
,RC_Number||'-'||DRIVER_ID as Driver_RC_key

from
( select x.*
,y.CREATED_AT owner_CREATED_AT
,row_number () over ( partition by PAN_of_GSTIN order by owner_CREATED_AT asc ) first_owner_sl
,y.first_name as owner_full_name
,y.MOBILE owner_mobile
,y.EMAIL owner_email
,y.uuid owner_uuid
,y.vendor_code,onboarding_source,owner_type,geo_region_id owner_geo_region_id
,case when first_owner_sl=1 then y.id else null end as primary_owner_find

from pan_base x 
left join  prod_curated.oms_public.partner_owners y on x.partner_owner_id=y.id
order by PAN_of_GSTIN, first_owner_sl asc ---,owner_CREATED_AT asc
) as p ) as w

left join enroll_data z on w.Driver_RC_key=z.Driver_RC_key
left join DEV_ELDORIA.CORE.WMI_CODE_LIST r on r.WMI_CODE=z.WMI_code


