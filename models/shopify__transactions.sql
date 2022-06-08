with transactions as (
    select 
        shopify_transaction.*,
        txn_receipt.receipt
    from {{ var('shopify_transaction') }} shopify_transaction
    LEFT JOIN {{ source('shopify', 'shopify_orders_refunds_transactions_receipts')}} txn_receipt
    USING(order_id)

), exchange_rate as (

    select
        *,
        coalesce(cast(nullif({{ fivetran_utils.json_parse("receipt",["charges","data",0,"balance_transaction","exchange_rate"]) }}, '') as {{ dbt_utils.type_numeric() }} ),1) as exchange_rate,
        coalesce(cast(nullif({{ fivetran_utils.json_parse("receipt",["charges","data",0,"balance_transaction","exchange_rate"]) }}, '') as {{ dbt_utils.type_numeric() }} ),1) * amount as currency_exchange_calculated_amount
    from transactions

)

select *
from exchange_rate