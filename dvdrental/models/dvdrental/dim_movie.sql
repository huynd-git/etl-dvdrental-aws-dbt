with stg_category as (
    select 
        fc.film_id,
        c.name
    from raw.dvdrental.film_category fc
    join raw.dvdrental.category c on (fc.category_id = c.category_id)
),

stg_film as (
    select * from raw.dvdrental.film
),

stg_language as (
    select * from raw.dvdrental.language
)

select 
    {{ dbt_utils.generate_surrogate_key(['f.film_id']) }} as movie_key,
    f.film_id,
    f.title,
    f.description,
    f.release_year,
    l.name as language,
    c.name as category,
    f.length,
    f.rating,
    f.special_features
from stg_film f
join stg_category c on (f.film_id = c.film_id)
join stg_language l on (f.language_id = l.language_id)


