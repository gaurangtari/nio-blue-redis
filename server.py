import redis
import json

r = redis.Redis()
last_id = 0
while(1):
    try:
            # Read data from the Redis stream
        response = r.xread({"nonJoyStream": last_id}, count=1, block=0)

        for stream_key, messages in response:
            for message_id, message_data in messages:
                    # Decode byte strings to regular strings
                message_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in message_data.items()}

                    # Print the full message to debug
                print(f"Full message: {message_data}")

                    # Check if 'data' exists in message_data
                if 'data' in message_data:
                        # Parse and print the message
                    data = json.loads(message_data['data'])  # Convert the JSON string to a Python dict
                    print(f"Message ID: {message_id}")
                    print(f"Data: {data}, Type: {type(data)}")
                    # Update last_id to the current message ID
                    last_id = message_id
                    e = 0
                
                else:
                    print(f"Error: 'data' key not found in message: {message_data}")
                    e = 0

    except redis.exceptions.ConnectionError as e: 
        e = e
        print(f"Failed to connect to Redis: {e}")
    except Exception as e:
        e =e
        print(f"Error reading from stream: {e}")
