with 
-- customers as (

--     select
--         id as customer_id,
--         first_name,
--         last_name

--     from raw.jaffle_shop.customers

-- ),

-- orders as (

--     select
--         id as order_id,
--         user_id as customer_id,
--         order_date,
--         status,
--         _etl_loaded_at

--     from raw.jaffle_shop.orders

-- ),

-- customer_orders as (

--     select
--         customer_id,

--         min(order_date) as first_order_date,
--         max(order_date) as most_recent_order_date,
--         count(order_id) as number_of_orders

--     from orders

--     group by 1

-- ),


-- final as (

--     select
--         customers.customer_id,
--         customers.first_name,
--         customers.last_name,
--         customer_orders.first_order_date,
--         customer_orders.most_recent_order_date,
--         coalesce(customer_orders.number_of_orders, 0) as number_of_orders

--     from customers

--     left join customer_orders using (customer_id)

-- ),

-- select * from final


-- create table "postgres"."public"."covid_epidemiology"
blabla as (
    select
        _airbyte_emitted_at,
        (current_timestamp at time zone 'utc')::timestamp as _airbyte_normalized_at,
        "_airbyte_data" as "data_raw",
        cast(jsonb_extract_path_text("_airbyte_data"::jsonb,'Lat') as varchar) as "lat",
        cast(jsonb_extract_path_text("_airbyte_data"::JSONB, 'Long') as varchar) as "long",
        cast(jsonb_extract_path_text("_airbyte_data"::JSONB, 'Imagename') as varchar) as "imagename",
        cast(jsonb_extract_path_text("_airbyte_data"::JSONB, 'Category') as varchar) as "category"
    from "dwtest".public.gps
),

-- create the category table
category as (
    select
        distinct category as category,
        row_number() over() as category_id
    from blabla
),

-- create the final table
final as (
    select
        blabla._airbyte_emitted_at,
        blabla._airbyte_normalized_at,
        blabla.data_raw,
        blabla.lat,
        blabla.long,
        blabla.imagename,
        blabla.category,
        category.category_id
    from blabla
    left join category on blabla.category = category.category
)

select * from final