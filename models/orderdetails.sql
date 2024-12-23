

select 
    od.order_id,
    od.product_id,
    od.unit_price,
    od.quantity,
    pr.product_name,
    pr.supplier_id,
    pr.category_id,
    round((od.unit_price * od.quantity),2) as total_sales_value,
    round(((pr.unit_price * od.quantity) - total_sales_value),2) as discount
from {{source('sources', 'order_details')}} od
left join {{source('sources', 'products')}} pr 
    ON (od.product_id = pr.product_id)
