# Retail Analytics Pipeline - Big Data Management

End-to-end data pipeline untuk analisis penjualan retail dan customer complaints menggunakan Apache Kafka, Spark, Delta Lake, dan Streamlit.

##  Arsitektur
```
Kagglehub CSV → Kafka → Delta Lake (Bronze/Silver/Gold) → Streamlit Dashboard
```

## Tech Stack

- **Ingestion:** Apache Kafka
- **Processing:** Apache Spark
- **Lakehouse:** Delta Lake
- **Warehouse:** Apache Hive (optional)
- **Visualization:** Streamlit

##  Setup

1. Clone repository
2. Masuk ke Folder scripts jalankan download_data.py untuk memasukkan ata
3. Jalankan: `docker-compose up -d`
4. Akses dashboard: `http://localhost:8501`

## Dashboard Features

- Sales vs Complaints Analysis
- Complaint Heatmap
- Regional Performance
- Interactive Filters
