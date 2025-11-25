#!/usr/bin/env python3
"""
Create MinIO Bucket using boto3 (AWS S3-compatible)
This works with MinIO's S3-compatible API
"""
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

# MinIO configuration
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
BUCKET_NAME = "data-lake"

def create_bucket():
    """Create bucket using boto3"""
    try:
        # Create S3 client with MinIO endpoint
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'  # MinIO doesn't care about region
        )
        
        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=BUCKET_NAME)
            print(f"✅ Bucket '{BUCKET_NAME}' already exists!")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                # Bucket doesn't exist, create it
                print(f"Creating bucket '{BUCKET_NAME}'...")
                s3_client.create_bucket(Bucket=BUCKET_NAME)
                print(f"✅ Bucket '{BUCKET_NAME}' created successfully!")
                return True
            else:
                print(f"❌ Error checking bucket: {error_code}")
                return False
        
    except ImportError:
        print("❌ boto3 not installed. Install it with:")
        print("   pip install boto3")
        return False
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print()
        print("Please create bucket manually via MinIO Console:")
        print("1. Open http://localhost:9001")
        print("2. Login: admin / admin123")
        print(f"3. Click 'Create Bucket' and enter: {BUCKET_NAME}")
        return False

def list_buckets():
    """List all buckets"""
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        response = s3_client.list_buckets()
        print("\nExisting buckets:")
        for bucket in response['Buckets']:
            print(f"  - {bucket['Name']}")
    except:
        pass

def main():
    print("=" * 60)
    print("Creating MinIO Bucket (using boto3)")
    print("=" * 60)
    print()
    print(f"MinIO Endpoint: {MINIO_ENDPOINT}")
    print(f"Bucket Name: {BUCKET_NAME}")
    print()
    
    if create_bucket():
        list_buckets()
        print()
        print("=" * 60)
        print("✅ Setup Complete!")
        print("=" * 60)
        print()
        print("Your bronze layer data will be stored at:")
        print(f"  s3a://{BUCKET_NAME}/bronze/validated/")
        print()
        print("Access MinIO Console:")
        print("  http://localhost:9001")
        print("  Login: admin / admin123")

if __name__ == "__main__":
    main()

