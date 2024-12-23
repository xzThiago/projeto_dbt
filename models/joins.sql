with prod as (
    select
        ct.category_name,
        sp.company_name as suppliers,
        pd.product_name,
        pd.unit_price,
        pd.product_id
    from {{source('sources', 'products')}} as pd 
    left join {{source('sources', 'suppliers')}} as sp 
        on (pd.supplier_id = sp.supplier_id)
    left join {{source('sources', 'categories')}} as ct 
        on (pd.category_id = ct.category_id)
), orddetails as (
    select
        pd.*,
        od.order_id,
        od.quantity,
        od.discount
    from {{ref('orderdetails')}} od 
    left join prod pd 
        on (od.product_id = pd.product_id)
), ordrs as (
    select
        ord.order_date,
        ord.order_id,
        cs.company_name as customer,
        em.full_name as employee,
        em.age,
        em.length_of_service
    from {{source('sources', 'orders')}} ord 
    left join {{ref('customers')}} cs
        on (ord.customer_id = cs.customer_id)
    left join {{ref('employees')}} em 
        on (ord.employee_id = em.employee_id)
    left join {{source('sources', 'shippers')}} sh 
        on (ord.ship_via = sh.shipper_id)
), finaljoin as (
    select
        od.*,
        ord.order_date,
        ord.customer,
        ord.employee,
        ord.age,
        ord.length_of_service
    from orddetails od
    inner join ordrs ord  
        on (od.order_id = ord.order_id)
)

select * from finaljoin
