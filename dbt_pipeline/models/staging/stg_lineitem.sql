SELECT 
    MD5(CONCAT(l_orderkey, '|', l_linenumber)) as item_surrogate_key,
    l_orderkey as order_key,
    l_partkey as part_key,
    l_linenumber as line_number,
    l_quantity as quantity,
    l_extendedprice as extended_price,
    l_discount as discount_percentage,
    l_tax as tax_rate,
FROM {{ source('tpch','lineitem') }}
