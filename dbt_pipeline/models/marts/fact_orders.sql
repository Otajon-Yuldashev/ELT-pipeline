SELECT 
    orders.total_price,
    orders.order_date,
    orders.customer_key,
    order_item_summary.*
FROM
   {{ ref('staging_tpch_orders') }} as orders
JOIN
   {{ ref('order_summary_mart') }} as order_item_summary
ON 
   orders.order_key = order_item_summary.order_key
order by order_date
