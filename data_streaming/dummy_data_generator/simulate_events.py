"""
Raw Data Generator - Video Recommendation System Events
Generates RAW video play events (like Netflix) with messy, unprocessed data
This data needs cleaning in bronze/silver/gold layers
"""
import json
import time 
import random
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})

def generate_video_event(timestamp_ms):
    """
    Generate raw video play event for recommendation system
    Best method for video recommendation - like Netflix/YouTube
    """
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "video_play_start",
        "timestamp": timestamp_ms,
        "timestamp_iso": datetime.now(timezone.utc).isoformat(),
        "user_id": f"user_{random.randint(1000, 99999)}",
        "session_id": f"sess_{random.randint(100000, 999999)}",
        
        # Device info (raw - might have missing fields)
        "device": {
            "type": random.choice(["mobile", "desktop", "tablet", "",None]),  # Sometimes null
            "os": random.choice(["iOS 17.1", "Android 14", "Windows 11", "macOS", ""]),  # Sometimes empty
            "browser": random.choice(["Safari", "Chrome", "Firefox", None]),
            "device_id": f"device_{random.randint(1000, 9999)}",
            "screen_resolution": f"{random.randint(1920, 3840)}x{random.randint(1080, 2160)}",  # Extra field
            "battery_level": random.randint(0, 100),  # Unnecessary for ML
        },
        
        # Content info (nested) - KEY for recommendation system
        "content": {
            "content_id": f"movie_{random.randint(80000000, 89999999)}",
            "title": random.choice(["Stranger Things", "The Crown", "Squid Game", "Breaking Bad", None]),  # Sometimes null
            "content_type": random.choice(["series", "movie", "documentary"]),
            "season": random.randint(1, 5) if random.random() > 0.3 else None,  # Sometimes missing
            "episode": random.randint(1, 10) if random.random() > 0.3 else None,
            "duration_seconds": random.randint(1800, 7200),
            "genre": random.choice(["Drama", "Thriller", "Comedy", "Action", ""]),  # Sometimes empty
            "release_year": random.randint(2010, 2024),  # Extra metadata
        },
        
        # Context (nested) - Important for recommendation
        "context": {
            "page": random.choice(["homepage", "search", "recommendations", "profile", None]),
            "referrer": random.choice(["search_results", "homepage", "external", ""]),
            "position": random.randint(1, 20),  # Position in recommendations
            "autoplay": random.choice([True, False, None]),  # Sometimes null
            "previous_content": f"movie_{random.randint(80000000, 89999999)}" if random.random() > 0.5 else None,  # Extra field
        },
        
        # Metadata (extra fields - might not be needed)
        "metadata": {
            "ip_address": f"{random.randint(192, 223)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "user_agent": random.choice(["Mozilla/5.0...", "Chrome/120.0...", "", None]),  # Sometimes empty/null
            "country": random.choice(["US", "UK", "CA", "IN", "DE", ""]),  # Sometimes empty
            "timezone": random.choice(["America/New_York", "Europe/London", "Asia/Kolkata", None]),
            "language": random.choice(["en", "es", "fr", "de", ""]),  # Sometimes empty
            "app_version": f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}",  # Extra field
        },
        
        # Raw fields that might be unnecessary
        "raw_data": {
            "http_method": "POST",  # Not needed for ML
            "request_id": str(uuid.uuid4()),  # Debugging only
            "server_timestamp": timestamp_ms + random.randint(-100, 100),  # Slight difference
        }
    }

# Main loop - generates video events continuously
while True:
    timestamp_ms = int(time.time() * 1000)
    
    # Generate video event (only one method - best for recommendation system)
    event = generate_video_event(timestamp_ms)
    
    # Send to Kafka
    producer.produce("user_events", json.dumps(event))
    producer.flush()
    
    print(f"Produced [video_play_start]:", json.dumps(event, indent=2)[:200] + "...")
    time.sleep(1)
