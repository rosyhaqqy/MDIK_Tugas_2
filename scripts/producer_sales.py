#!/usr/bin/env python3
"""
Producer untuk Superstore Sales Data (Structured)
Membaca CSV dan streaming ke Kafka topic
"""
import pandas as pd
from kafka import KafkaProducer
import json
import time
from datetime import datetime

def create_producer():
    """Inisialisasi Kafka Producer"""
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        compression_type='gzip',
        batch_size=16384,
        linger_ms=10
    )

def stream_sales_data(file_path, topic_name, batch_size=100):
    """Stream sales data ke Kafka"""
    producer = create_producer()
    
    print(f"📊 Reading sales data from {file_path}")
    df = pd.read_csv(file_path, encoding='ISO-8859-1')
    
    # Data cleaning
    df['Order Date'] = pd.to_datetime(df['Order Date'], format='%d/%m/%Y', errors='coerce')
    df['Ship Date'] = pd.to_datetime(df['Ship Date'], format='%d/%m/%Y', errors='coerce')
    
    print(f"📤 Streaming {len(df)} records to Kafka topic: {topic_name}")
    
    success_count = 0
    for idx, row in df.iterrows():
        try:
            # Konversi row ke dictionary
            message = {
                'row_id': int(row['Row ID']),
                'order_id': str(row['Order ID']),
                'order_date': row['Order Date'].isoformat() if pd.notna(row['Order Date']) else None,
                'ship_date': row['Ship Date'].isoformat() if pd.notna(row['Ship Date']) else None,
                'ship_mode': str(row['Ship Mode']),
                'customer_id': str(row['Customer ID']),
                'customer_name': str(row['Customer Name']),
                'segment': str(row['Segment']),
                'country': str(row['Country']),
                'city': str(row['City']),
                'state': str(row['State']),
                'postal_code': str(row['Postal Code']) if pd.notna(row['Postal Code']) else None,
                'region': str(row['Region']),
                'product_id': str(row['Product ID']),
                'category': str(row['Category']),
                'sub_category': str(row['Sub-Category']),
                'product_name': str(row['Product Name']),
                'sales': float(row['Sales']),
                'quantity': int(row['Quantity']),
                'discount': float(row['Discount']),
                'profit': float(row['Profit']),
                'ingestion_timestamp': datetime.now().isoformat()
            }
            
            # Send ke Kafka
            producer.send(topic_name, value=message)
            success_count += 1
            
            # Progress indicator
            if (idx + 1) % batch_size == 0:
                producer.flush()
                print(f"  ✓ Sent {idx + 1}/{len(df)} records")
                time.sleep(0.1)  # Rate limiting
                
        except Exception as e:
            print(f"  ✗ Error at row {idx}: {e}")
            continue
    
    producer.flush()
    producer.close()
    print(f"\n✅ Ingestion completed! {success_count}/{len(df)} records sent successfully")

if __name__ == "__main__":
    stream_sales_data(
        file_path='../data/superstore.csv',
        topic_name='superstore-sales',
        batch_size=100
    )
