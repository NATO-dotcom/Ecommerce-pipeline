import pandas as pd
import json
import time
import sys
from confluent_kafka import Producer

#Kafka configurations
KAFKA_TOPIC = 'olist.orders'
KAFKA_CONFIG = {
            'bootstrap.servers' : 'localhost:9092',
            'client.id' : 'olist-producer'
}

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed {err}")
    else:
        print(f"Message order{msg.key().decode('utf-8')}to partitions {msg.partition()}")

def prepare_data():
    print("Loading and joining CSV datasets")
    
    orders = pd.read_csv("data/raw/olist_orders_dataset.csv")
    items = pd.read_csv("data/raw/olist_order_items_dataset.csv")
    customers = pd.read_csv("data/raw/olist_customers_dataset.csv")
    
    #Join datasets
    df = orders.merge(customers, on='customer_id', how='inner')
    df = df.merge(items, on='order_id', how='inner')
    
    # One event per order with the total_price.
    agg_df = df.groupby(
        ['order_id', 'customer_state', 'order_purchase_timestamp']
    )['price'].sum().reset_index()

    agg_df.rename(columns={'price': 'total_price'}, inplace=True)
    

    agg_df = agg_df.sort_values('order_purchase_timestamp')
    return agg_df

def main():
    producer = Producer(KAFKA_CONFIG)
    events_df = prepare_data()

    print(f"Starting stream replay for {len(events_df)} orders...")

    for index, row in events_df.iterrows():
        # Build the JSON payload
        event_payload = {
            "order_id": row['order_id'],
            "customer_state": row['customer_state'],
            "total_price": row['total_price'],
            "purchase_timestamp": row['order_purchase_timestamp']
        }

        # Convert Python dictionary to JSON string, then encode to bytes
        json_data = json.dumps(event_payload).encode('utf-8')
        # Encode the key to bytes
        message_key = row['order_id'].encode('utf-8')

        # Asynchronously produce the message
        producer.produce(
            topic=KAFKA_TOPIC,
            key=message_key,
            value=json_data,
            callback=delivery_report
        )

        # Trigger any available delivery report callbacks from previous produce() calls
        producer.poll(0)

        # Pause to simulate real-time streaming
        time.sleep(0.5)

    # Block until all messages are sent
    print("Flushing final messages to Kafka...")
    producer.flush()

if __name__ == '__main__':
    main()