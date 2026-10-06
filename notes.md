# Olist Dataset: Exploration Notes & Cleaning Rules

## 1. Null Values
*   **Orders Dataset:** Missing values found in timestamp columns (`order_approved_at`, `order_delivered_carrier_date`, `order_delivered_customer_date`). 
    *   *Cleaning Rule:* Keep null delivery dates for non-delivered orders, but add an `is_delivered` boolean flag[cite: 3].
*   **Products Dataset:** Missing values found in `product_category_name` and physical dimension columns.
    *   *Cleaning Rule:* Fill missing categories with the string "unknown" after joining the English translation table[cite: 3].

## 2. Duplicates
*   **Orders Dataset:** No duplicate `order_id`s found.
*   **Reviews Dataset:** Duplicate `review_id`s exist (likely due to a single review applying to multiple items in an order)[cite: 2].
    *   *Cleaning Rule:* Drop duplicate reviews[cite: 3].
*   **Customers Dataset:** (Assumed duplicates based on project guide requirements).
    *   *Cleaning Rule:* Drop duplicate customers[cite: 3].

## 3. Schema & Parsing Issues
*   **Timestamps:** Currently inferred as string data types across all tables.
    *   *Cleaning Rule:* Cast `order_purchase_timestamp`, `order_delivered_customer_date`, and `order_estimated_delivery_date` using `to_timestamp`[cite: 3].
*   **CSV Parsing (Reviews):** Reviews contain multi-line text and line breaks that corrupt standard CSV reading.
    *   *Ingestion Rule:* Must use `multiLine=True` and `escape='"'` when reading the raw data.
*   **Text Formatting:** City names and text columns are inconsistently cased.
    *   *Cleaning Rule:* Trim and standardize text columns (e.g., convert city names to lowercase)[cite: 3].

## 4. Business Logic Filtering
*   **Order Items:** Some items may have missing or zero prices.
    *   *Cleaning Rule:* Remove items with a price `<= 0`[cite: 3].