WITH PRE_PIVOT as ( 
    select
     customer_id
 , split_part(pivot_columns, '___', 1) as device
 , split_part(pivot_columns, '___', 2) as factor
 , value
FROM (SELECT * FROM pd2023_wk06_dsb_customer_survey) as src
UNPIVOT(
value for pivot_columns IN (
MOBILE_APP___EASE_OF_USE, MOBILE_APP___EASE_OF_ACCESS, MOBILE_APP___NAVIGATION, MOBILE_APP___LIKELIHOOD_TO_RECOMMEND, MOBILE_APP___OVERALL_RATING, ONLINE_INTERFACE___EASE_OF_USE, ONLINE_INTERFACE___EASE_OF_ACCESS, ONLINE_INTERFACE___NAVIGATION, ONLINE_INTERFACE___LIKELIHOOD_TO_RECOMMEND, ONLINE_INTERFACE___OVERALL_RATING
)) as pivot
) , formatted_data as (
select * 
, AVG(MOBILE_APP) OVER (PARTITION by customer_id) as av_mobile_score
, AVG(ONLINE_INTERFACE) OVER (PARTITION by customer_id) as av_online_score
, av_mobile_score - av_online_score as ratings_diff
, CASE
    when ratings_diff >= 2 then 'Mobile SuperFan'
    when ratings_diff >= 1 then 'Mobile Fan'
    when ratings_diff <= -2 then 'Online Fan'
    when ratings_diff <= -1 then 'Online SuperFan'
    ELSE 'Neutral'
    end as fan
from pre_pivot
pivot (sum(value) for device in ('MOBILE_APP', 'ONLINE_INTERFACE')) as p
           (customer_id, factor, mobile_app, online_interface)
where factor != 'OVERALL_RATING')

select 
fan
 , ROUND((( 
 count(distinct customer_id)
 / ( select count(distinct customer_id) from formatted_data )
 ) * 100), 1) as Percent_of_Total
 
from formatted_data
group by fan