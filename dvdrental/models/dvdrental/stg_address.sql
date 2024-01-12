select
    a.address_id,
    a.address,
    a.address2,
    a.district,
    a.postal_code,
    a.phone,
    ci.city,
    co.country
from
    raw.dvdrental.address a
join raw.dvdrental.city ci on (a.city_id = ci.city_id)
join raw.dvdrental.country co on (ci.country_id = co.country_id)