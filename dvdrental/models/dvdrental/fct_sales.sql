-- create fct_sales table
with stg_payment as (
    select 
        payment_id,
        customer_id,
        rental_id,
        amount,
        DATE(payment_date) as date
    from raw.dvdrental.payment
),

stg_rental as (
    select
        r.rental_id,
        i.film_id,
        i.store_id
    from raw.dvdrental.rental r
    join raw.dvdrental.inventory i on (r.inventory_id = i.inventory_id)
)

select
    {{ dbt_utils.generate_surrogate_key(['p.payment_id']) }} as sales_key,
    {{ dbt_utils.generate_surrogate_key(['p.date']) }} as date_key,
    {{ dbt_utils.generate_surrogate_key(['p.customer_id'] )}} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['r.film_id']) }} as movie_key,
    {{ dbt_utils.generate_surrogate_key(['r.store_id']) }} as store_key,
    p.amount as sales_amount
from stg_payment p
join stg_rental r on (p.rental_id = r.rental_id)
