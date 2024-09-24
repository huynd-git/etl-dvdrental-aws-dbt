-- create dim_date table
with stg_date as (
    select distinct date(payment_date) as date from raw.dvdrental.payment
)

select 
    {{ dbt_utils.generate_surrogate_key(['date']) }} as date_key,
    date,
    MONTH(date) as month,
    QUARTER(date) as quarter,
    YEAR(date) as year,
    WEEK(date) as week,
    DAY(date) as day,
    CASE WHEN DAYOFWEEKISO(date) IN (6, 7) THEN true ELSE false END as is_weekend
from
    stg_date
