#@namespace scala com.rouesnel.typedsql.examples.coppersmith

struct OrderLineItem {
  1: required string order_id
  2: required i32    order_item_id
  3: required i32    product_id
  4: required string product_name
  5: required i32    item_quantity
  6: required i32    item_price
  7: optional i32    item_discount
  8: optional string coupon_code
  9: optional i32    coupon_amount
}

struct Order {
  1: required string order_id
  2: required i32    customer_id
  3: required i32    order_time
  4: optional i32    delivered_at
  5: optional i32    cancelled
  6: optional i32    refunded_at
}

struct Payment {
  1: required string payment_id
  2: required string order_id
  3: required i32    amount
}

struct Customer {
  1: required i32    customer_id
  2: required string first_name
  3: required string last_name
  4: required i32    date_joined
  5: required string email_address
  6: optional string gender
  7: required i32    age
}