SELECT 
    line_item.item_surrogate_key,
    line_item.part_key,
    line_item.line_number,
    line_item.extended_price,
    orders.order_key,
    orders.customer_key,
    orders.order_date,
    {{ discounted_amount('line_item.extended_price', 'line_item.discount_percentage') }} as item_discount_amount
FROM 
   {{ ref('staging_tpch_orders') }} as orders
JOIN 
   {{ ref('stg_lineitem') }} as line_item 
    on orders.order_key = line_item.order_key 
ORDER BY order_date
