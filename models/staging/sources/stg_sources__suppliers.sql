with 

source as (

    select * from {{ source('sources', 'suppliers') }}

),

renamed as (

    select
        supplier_id,
        company_name,
        contact_name,
        contact_title,
        address,
        city,
        region,
        postal_code,
        country,
        phone,
        fax,
        homepage

    from source

)

select * from renamed
