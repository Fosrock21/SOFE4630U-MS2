import csv
import json
import glob
import os
import random
import time
from google.cloud import pubsub_v1

# --- CONFIGURATION ---
project_id = "steam-collector-485722-i5"
topic_id = "smartMeterReadings"

# --- AUTHENTICATION ---
files = glob.glob("*.json")
if not files:
    raise Exception(
        "No JSON key file found! Make sure your service account key is in this folder.")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

print(f"Reading data from Labels.csv...")

# Function to safely convert strings to numbers (or None if empty)


def clean_value(value):
    if not value or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


# --- MAIN LOOP ---
with open('Labels.csv', mode='r') as csv_file:
    # Read the CSV rows
    csv_reader = csv.DictReader(csv_file)

    for row in csv_reader:

        # 1. GENERATE ID (Critical: Database requires this!)
        # We generate a random integer ID just like smartMeter.py did
        generated_id = random.randint(1000000, 9999999)

        # 2. TRANSFORM DATA (The Fix)
        # We map the CSV columns (Left) to the Database columns (Right)
        structured_message = {
            "ID": generated_id,
            "time": int(clean_value(row['time'])),       # Convert to Int
            # Rename 'profileName' -> 'profile_name'
            "profile_name": row['profileName'],
            "temperature": clean_value(row['temperature']),
            "humidity": clean_value(row['humidity']),
            "pressure": clean_value(row['pressure'])
        }

        # 3. Serialize to JSON
        message_json = json.dumps(structured_message)
        message_bytes = message_json.encode('utf-8')

        # 4. Publish
        try:
            publish_future = publisher.publish(topic_path, message_bytes)
            publish_future.result()  # Wait for confirmation
            print(
                f"Sent ID {generated_id}: {structured_message['profile_name']}")
        except Exception as e:
            print(f"Failed to send: {e}")

        # Optional: Sleep slightly so you can watch it happen
        time.sleep(0.1)

print("\nAll CSV data sent successfully!")
