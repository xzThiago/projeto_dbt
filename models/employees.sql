with calc_employees as (
    select
        extract(year from current_date) - extract(year from birth_date) as age,
        extract(year from current_date) - extract(year from hire_date) as length_of_service,
        (first_name || ' ' || last_name) as full_name,
        *
    from {{source('sources', 'employees')}} 
)
select * from calc_employees