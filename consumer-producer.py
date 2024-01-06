from kafka import KafkaConsumer, KafkaProducer
import json, time

# Initialize Kafka consumer

consumer = KafkaConsumer('topic1', bootstrap_servers='localhost:9092', group_id='consumer-producer-group', max_poll_records=5)

# Initialize Kafka producer  

producer = KafkaProducer(bootstrap_servers='localhost:9092', acks='all')

# Initialize variables

received_data_count = 0
total_temperature = 0

# Read, process and send data

for message in consumer:
    # Decode and load JSON data from the Kafka message
    weather_data = message.value.decode('utf-8')
    weather_data_json = json.loads(weather_data)

    # Add data to variables
    received_data_count += 1
    total_temperature += weather_data_json.get('main', {}).get('temp', 0)
    
    temperature = weather_data_json.get('main', {}).get('temp', 0)
    print(f"Message number: {received_data_count} -> Temperature: {temperature}")

    # If 5 messages have been received, calculate the average temperature and send it
    if received_data_count == 5:
        average_temperature = total_temperature / 5 if received_data_count > 0 else 0

        # Send data to Kafka another topic
        processed_data = {'average_temperature': average_temperature}
        producer.send('topic2', value=json.dumps(processed_data).encode('utf-8'))

        print(f"Average temperature: {average_temperature}")

        # Reset values
        received_data_count = 0
        total_temperature = 0

# Close the consumer and the producer (this part will not be reached in this example)

consumer.close()
producer.close()
