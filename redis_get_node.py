import redis
import rospy
from std_msgs.msg import Float64
import json
from datetime import datetime
from auv_msgs.msg import NavigationStatus  
import pytz


class Datagetter :
    def __init__(self):
        print("Data getter class")
        rospy.init_node("data_getter_node")
        self.db = None
        self.data = { "surge": 0, "sway": 0, "heave": 0, "yaw": 0 }
        self.stream_name = "joystickStream"
        self.stream_name_state = "vehicle_state"
        self.velocity= [0,0,0]
        self.orientation = [0,0,0]
        self.state = {"northing":0,"easting":0,"depth":0,"velocity":self.velocity,"orientation":self.orientation}
        self.last_id = 0
        self.e = 0
        self.depth = 0
        self.init_timers()
        self.init_publishers()
        self.init_subscribers()
        self.database_init()

    def init_publishers(self):
        self.surge_pub = rospy.Publisher("/bluerov_heavy0/ref/surge",Float64,queue_size=5)
        self.sway_pub = rospy.Publisher("/bluerov_heavy0/ref/sway",Float64,queue_size=5)
        self.heave_pub = rospy.Publisher("/bluerov_heavy0/ref/depth",Float64,queue_size=5)
        self.yaw_pub = rospy.Publisher("/bluerov_heavy0/ref/yaw_rate",Float64,queue_size=5)
        print("initializing subscribers")

    def init_timers(self):
        self.timer = rospy.Timer(rospy.Duration(0.1),self.timer_callback)
        self.pub_timer = rospy.Timer(rospy.Duration(0.025),self.pub_timer_callback)
        print("initializing timers")

    def init_subscribers(self):
        self.state_sub = rospy.Subscriber("/bluerov_heavy0/nav/filter/state",NavigationStatus,self.state_callback)
        
    def database_init(self):
        self.db = redis.Redis.from_url(url="redis://default:jhnr76GXIDfoqtETx8GGIi9PeNqBHYwe@redis-12396.c261.us-east-1-4.ec2.redns.redis-cloud.com:12396")
        print("initializing database")
    
    def timer_callback(self,event):
        self.database_read()
        # print(self.data)
    
    def database_read(self):
        try:
            # Read data from the Redis stream
            response = self.db.xread({self.stream_name: self.last_id}, count=1, block=0)

            for stream_key, messages in response:
                for message_id, message_data in messages:
                    # Decode byte strings to regular strings
                    message_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in message_data.items()}

                    # Print the full message to debug
                    print(f"Full message: {message_data}")

                    # Check if 'data' exists in message_data
                    if 'data' in message_data:
                        # Parse and print the message
                        self.data = json.loads(message_data['data'])  # Convert the JSON string to a Python dict
                        print(f"Message ID: {message_id}")
                        print(f"Data: {self.data}, Type: {type(self.data)}")
                        print(f"[{self.get_time()}] Surge: {self.data['surge']}, Sway: {self.data['sway']}, Heave: {self.data['heave']}, Yaw: {self.data['yaw']}")
                        self.depth += -self.data["heave"]
                        # Update last_id to the current message ID
                        self.last_id = message_id
                        self.e = 0
                    else:
                        print(f"Error: 'data' key not found in message: {message_data}")
                        self.e = 0

        except redis.exceptions.ConnectionError as e: 
            self.e = e
            print(f"Failed to connect to Redis: {e}")
        except Exception as e:
            self.e =e
            print(f"Error reading from stream: {e}")

    def pub_timer_callback(self,event):
        if not(self.e) :
            self.surge_pub.publish(Float64(5*self.data['surge']))
            self.sway_pub.publish(Float64(5*self.data['sway']))
            self.heave_pub.publish(Float64(0.5*self.depth))
            self.yaw_pub.publish(Float64(5*self.data['yaw']))
    
    def state_callback(self,msg):
        try:
            self.state["northing"] = msg.position.north
            self.state["easting"] = msg.position.east
            self.state["depth"] = msg.position.depth
            self.state["velocity"] = [msg.seafloor_velocity.x, msg.seafloor_velocity.y, msg.seafloor_velocity.z]
            self.state["orientation"] = [msg.orientation.x, msg.orientation.y, msg.orientation.z]

            # Convert lists to JSON strings before storing in Redis
            state_to_store = {
            "northing": self.state["northing"],
            "easting": self.state["easting"],
            "depth": self.state["depth"],
            "velocity": json.dumps(self.state["velocity"]),
            "orientation": json.dumps(self.state["orientation"]),
            }
            print("debug")
            self.db.xadd("vehicle_state",state_to_store)
            # for stream_key, messages in response:
            #     for message_id, message_data in messages:
            #         # Decode byte strings to regular strings
            #         message_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in message_data.items()}

            #         # Print the full message to debug
            #         print(f"Full message: {message_data}")

            #         # Check if 'data' exists in message_data
            #         if 'data' in message_data:
            #             # Parse and print the message
            #             self.data = json.loads(message_data['data'])  # Convert the JSON string to a Python dict
            #             print(f"Message ID: {message_id}")
            #             print(f"Data: {self.data}, Type: {type(self.data)}")
            #             print(f"[{self.get_time()}] Surge: {self.data['surge']}, Sway: {self.data['sway']}, Heave: {self.data['heave']}, Yaw: {self.data['yaw']}")
            #             self.depth += -self.data["heave"]
            #             # Update last_id to the current message ID
            #             self.last_id = message_id
            #             self.e = 0
            #         else:
            #             print(f"Error: 'data' key not found in message: {message_data}")
            #             self.e = 0

            print(self.state)
        except redis.exceptions.ConnectionError as e: 
            self.e = e
            print(f"Failed to connect to Redis: {e}")
        except Exception as e:
            self.e =e
            print(f"Error reading from stream: {e} \n {self.state}")

    
    def get_time(self):

        # Get the current UTC time
        utc_time = datetime.now(pytz.UTC)

        # Define the IST timezone
        ist_timezone = pytz.timezone('Asia/Kolkata')

        # Convert the current UTC time to IST
        ist_time = utc_time.astimezone(ist_timezone)

        # Format the IST datetime object in ISO 8601 format with timezone offset
        ist_time_str = ist_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + ist_time.strftime('%z')
        ist_time_str = ist_time_str[:22] + ':' + ist_time_str[22:]

        return ist_time_str

        


if __name__ == "__main__":
    dg =  Datagetter()
    rospy.spin()