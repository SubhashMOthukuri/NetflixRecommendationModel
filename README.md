# NetflixRecommendationModel

Production-grade ML Data Pipeline for Netflix-style recommendation system

## Pipeline Architecture

- Producer → Kafka
- Kafka → Spark → Bronze Layer
- Bronze → Silver (coming soon)
- Silver → Gold → Feature Store (coming soon)
