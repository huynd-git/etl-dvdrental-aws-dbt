with addressInfo as (
    select * from {{ ref('stg_address') }}
),

stg_store as (
    select * from raw.dvdrental.store
),

stg_staff as (
    select * from raw.dvdrental.staff
)

select
    {{ dbt_utils.generate_surrogate_key(['s.store_id']) }} as store_key,
    s.store_id,
    a.address,
    a.address2,
    a.district,
    a.city,
    a.country,
    a.postal_code,
    a.phone,
    st.first_name as manager_first_name,
    st.last_name as manager_last_name
from stg_store s
join addressInfo a on (s.address_id = a.address_id)
join stg_staff st on (s.manager_staff_id = st.staff_id)
