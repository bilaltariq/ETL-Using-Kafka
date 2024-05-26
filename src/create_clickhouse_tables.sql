CREATE DATABASE IF NOT EXISTS ecommerce;

CREATE TABLE ecommerce.products (
    id UInt64,
    name String,
    description String,
    sku String,
    price Float64,
    created_at DateTime,
    modified_at DateTime,
    status String--,
    --fetch_timestamp DATETIME
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE ecommerce.customers (
    id UInt64,
    name String,
    age UInt8,
    address String,
    city String,
    created_at DateTime,
    modified_at DateTime,
    phone_number String--,
    --fetch_timestamp DATETIME
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE ecommerce.orders (
    id UInt64,
    order_create_date DateTime,
    billing_address String,
    shipping_address String,
    city String,
    customer_id UInt64,
    payment_method String--,
    --fetch_timestamp DATETIME
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE ecommerce.order_items (
    id UInt64,
    order_id UInt64,
    unit_price Float64,
    discount_price Float64,
    paid_price Float64,
    shipping_fee Float64,
    item_status String,
    item_create_date DateTime,
    item_modified_date DateTime,
    delivery_date Nullable(DateTime),
    quantity UInt32,
    product_sku_code String
) ENGINE = MergeTree()
ORDER BY id;
