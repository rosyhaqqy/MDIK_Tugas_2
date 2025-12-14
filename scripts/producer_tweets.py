#!/usr/bin/env python3
"""
Producer untuk Customer Support Tweets (Unstructured)
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
        compression_type='gzip'
    )

def stream_tweets_data(file_path, topic_name, batch_size=50):
    """Stream tweets data ke Kafka"""
    producer = create_producer()
    
    print(f"💬 Reading tweets data from {file_path}")
    df = pd.read_csv(file_path)
    
    # Filter hanya tweets dari customers (bukan response dari company)
    df = df[df['inbound'] == True].copy()
    
    print(f"📤 Streaming {len(df)} tweets to Kafka topic: {topic_name}")
    
    success_count = 0
    for idx, row in df.iterrows():
        try:
            message = {
                'tweet_id': str(row['tweet_id']),
                'author_id': str(row['author_id']),
                'text': str(row['text']),
                'response_tweet_id': str(row['response_tweet_id']) if pd.notna(row['response_tweet_id']) else None,
                'in_response_to_tweet_id': str(row['in_response_to_tweet_id']) if pd.notna(row['in_response_to_tweet_id']) else None,
                'created_at': str(row['created_at']) if pd.notna(row['created_at']) else None,
                'ingestion_timestamp': datetime.now().isoformat()
            }
            
            producer.send(topic_name, value=message)
            success_count += 1
            
            if (idx + 1) % batch_size == 0:
                producer.flush()
                print(f"  ✓ Sent {idx + 1}/{len(df)} tweets")
                time.sleep(0.1)
                
        except Exception as e:
            print(f"  ✗ Error at row {idx}: {e}")
            continue
    
    producer.flush()
    producer.close()
    print(f"\n✅ Ingestion completed! {success_count}/{len(df)} tweets sent successfully")

if __name__ == "__main__":
    stream_tweets_data(
        file_path='../data/tweets.csv',
        topic_name='customer-tweets',
        batch_size=50
    )
