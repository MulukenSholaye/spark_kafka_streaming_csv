import csv
import json
import time
from kafka import KafkaProducer

# Define Kafka producer configuration
# Replace 'localhost:9092' with your Kafka broker's address if it's different.
KAFKA_BROKERS = ['localhost:9092']
KAFKA_TOPIC = 'invoices'

def create_kafka_producer():
    """
    Creates and returns a Kafka producer instance.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        return producer
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        return None

def publish_invoices_to_kafka(csv_file_path, producer):
    """
    Reads a CSV file row by row and publishes each row as a JSON message to Kafka.
    """
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                # Convert all values to appropriate types
                invoice_data = {
                    "InvoiceID": int(row["InvoiceID"]),
                    "SellerID": int(row["SellerID"]),
                    "BuyerID": int(row["BuyerID"]),
                    "TaxID": int(row["TaxID"]),
                    "DeviceID": int(row["DeviceID"]),
                    "Payment_methodID": int(row["Payment_methodID"]),
                    "CancelReasonID": float(row["CancelReasonID"]) if row["CancelReasonID"] else None,
                    "ErrorTypeID": float(row["ErrorTypeID"]) if row["ErrorTypeID"] else None,
                    "InvoiceTotal_amount": float(row["InvoiceTotal_amount"]),
                    "subtotal_amount": float(row["subtotal_amount"]),
                    "tax_amount": float(row["tax_amount"]),
                    "discount_amount": float(row["discount_amount"]),
                    "invoice_Date": row["invoice_Date"],
                    "invoice_ModifiedDate": row["invoice_ModifiedDate"]
                }
                
                # Publish the JSON object to the Kafka topic
                producer.send(KAFKA_TOPIC, value=invoice_data)
                print(f"Published invoice with ID {invoice_data['InvoiceID']}")
                
                # Add a short delay to simulate a stream of data
                time.sleep(0.1)

    except FileNotFoundError:
        print(f"Error: The file {csv_file_path} was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # Flush and close the producer to ensure all messages are sent
        if producer:
            producer.flush()
            producer.close()

if __name__ == "__main__":
    # Path to the CSV file you want to publish
    csv_path = 'Fact_Invoice.csv'
    
    # Create the Kafka producer
    producer = create_kafka_producer()

    if producer:
        # Publish the data to Kafka
        publish_invoices_to_kafka(csv_path, producer)
