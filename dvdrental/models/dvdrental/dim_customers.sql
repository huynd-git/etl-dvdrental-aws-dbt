with stg_customer as (
    select * from raw.dvdrental.customer
),

addressInfo as (
    select * from {{ ref('stg_address') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} as customer_key,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    a.address,
    a.address2,
    a.district,
    a.city,
    a.country,
    a.postal_code,
    a.phone,
    c.create_date,
    c.active
from stg_customer c
join addressInfo a on (c.address_id = a.address_id)