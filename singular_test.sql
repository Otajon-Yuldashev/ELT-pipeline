SELECT 
  * 
FROM
  {{ ref('fact_orders') }}
WHERE
   item_discount_amount > 0