import json
import math
import psycopg2
from datetime import datetime
from kafka_project.KafkaUtil.consumer import Consumer

# connection = psycopg2.connect(database="postgres", user="postgres", password="admin", host="localhost", port=5432)
# cursor = connection.cursor()

def upload_to_db(consumer: Consumer):
    connection = psycopg2.connect(database="postgres", user="postgres", password="admin", host="localhost", port=5432)
    cursor = connection.cursor()
    record_list = consumer.consume_messages(["transactions_val"])
    for record in record_list:
        record = json.loads(record)

        # Convert types
        trans_time = datetime.strptime(record["trans_date_trans_time"], "%m/%d/%Y %H:%M")
        dob = datetime.strptime(record["dob"], "%d/%m/%Y").date()

        # Cast numbers safely
        cc_num = int(record["cc_num"])
        if not (math.isnan(record.get("merch_zipcode"))):
            merch_zipcode = int(record["merch_zipcode"])
        else:
            merch_zipcode =None
        cursor.execute("""
                INSERT INTO transactions (
                    transaction_id, trans_date_trans_time, cc_num, merchant, category, amt,
                    first, last, gender, street, city, state, zip, lat, long,
                    city_pop, job, dob, trans_num, unix_time, merch_lat,
                    merch_long, is_fraud, merch_zipcode, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record["id"], trans_time, cc_num, record["merchant"], record["category"], record["amt"],
                record["first"], record["last"], record["gender"], record["street"], record["city"], record["state"],
                int(record["zip"]), record["lat"], record["long"], record["city_pop"], record["job"],
                dob, record["trans_num"], record["unix_time"], record["merch_lat"], record["merch_long"],
                record["is_fraud"], merch_zipcode, record["status"]
            ))

    connection.commit()
    cursor.close()
    connection.close()
    print("Transactions inserted to db")

def fetch_from_db():
    # Initialise connection to PostgreSQL database
    connection = psycopg2.connect(database="postgres", user="postgres", password="admin", host="localhost", port=5432)
    cursor = connection.cursor()
    cursor.execute("SELECT * from transactions;")
    # Fetch all rows from database
    records = cursor.fetchall()

    print(f"No. of records: {len(records)}")
    cursor.close()
    connection.close()
    return records
