import redis
import rospy
from std_msgs.msg import Float64
import json
from datetime import datetime
from auv_msgs.msg import NavigationStatus 
from waypoint.srv import sendWpType1 
import pytz


class Datagetter :
    def __init__(self):
        print("Data getter class")
        rospy.init_node("data_getter_node")
        self.db = None
        self.data = { "surge": 0, "sway": 0, "heave": 0, "yaw": 0 }
        self.stream_name = "joystickStream"
        self.channel = "joystick-data"
        self.stream_name_state = "vehicle_state"
        self.velocity= [0,0,0]
        self.orientation = [0,0,0]
        self.max_depth = 10
        self.state = {"northing":0,"easting":0,"depth":0,"velocity":self.velocity,"orientation":self.orientation}
        self.teleop_data = {}
        self.last_id = 0
        self.e = 0
        self.teleop_mode = 0
        self.depth = 0
        self.pubsub = None
        self.init_timers()
        self.init_publishers()
        self.init_subscribers()
        self.database_init()

    def limit_val(self,val,x,y):
        if val > x:
            return x
        else :
            if val < y:
                return y
            return val
        
    def init_publishers(self):
        self.surge_pub = rospy.Publisher("/bluerov_heavy0/ref/surge",Float64,queue_size=5)
        self.sway_pub = rospy.Publisher("/bluerov_heavy0/ref/sway",Float64,queue_size=5)
        self.heave_pub = rospy.Publisher("/bluerov_heavy0/ref/depth",Float64,queue_size=5)
        self.yaw_pub = rospy.Publisher("/bluerov_heavy0/ref/yaw_rate",Float64,queue_size=5)
        print("initializing subscribers")

    def init_timers(self):
        self.timer = rospy.Timer(rospy.Duration(0.1),self.timer_callback)
        self.pub_timer = rospy.Timer(rospy.Duration(0.025),self.pub_timer_callback)
        self.teleop_timer = rospy.Timer(rospy.Duration(0.1),self.teleop_callback)
        print("initializing timers")

    def init_subscribers(self):
        self.state_sub = rospy.Subscriber("/bluerov_heavy0/nav/filter/state",NavigationStatus,self.state_callback)
        # self.state = 0

    def init_services(self):
        self.waypoint_srv = rospy.ServiceProxy("bluerov_heavy0/controls/send_wp_standard",sendWpType1,persistent=True)
    
    def database_init(self):
        # self.db = redis.Redis.from_url(url="redis://default:jhnr76GXIDfoqtETx8GGIi9PeNqBHYwe@redis-12396.c261.us-east-1-4.ec2.redns.redis-cloud.com:12396")
        self.db = redis.Redis()
        self.pubsub = self.db.pubsub()
        print("initializing database")
    
    def timer_callback(self,event):
        self.database_read()
        # print(self.data)
    
    def database_read(self):
        try:
            # Read data from the Redis stream
            x = self.pubsub.subscribe(self.channel)
            for message in self.pubsub.listen():
                if message['type'] == 'message':
                # Print the received message
                    self.data = json.loads(message['data'])
                    self.depth += -0.1*self.data["heave"]
                    # print(self.depth)
                    self.depth = self.limit_val(self.depth,self.max_depth,0)
                    print(self.depth)
            print("Subscription stopped by user.")
            self.teleop_mode = 0
        except redis.exceptions.ConnectionError as e: 
            self.e = e
            print(f"Failed to connect to Redis: {e}")
        except Exception as e:
            self.e =e
            print(f"Error reading from stream: {e} \n {self.state}")
        finally:
            self.pubsub.close()
    
    def teleop_read(self):
        try:
                # Read data from the Redis stream
            response = self.db.xread({"nonJoyStream": self.last_id}, count=1, block=0)
            self.teleop_mode = 1
            for stream_key, messages in response:
                for message_id, message_data in messages:
                        # Decode byte strings to regular strings
                    message_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in message_data.items()}

                        # Print the full message to debug
                    print(f"Full message: {message_data}")

                        # Check if 'data' exists in message_data
                    if 'data' in message_data:
                            # Parse and print the message
                        self.teleop_data = json.loads(message_data['data'])  # Convert the JSON string to a Python dict
                        print(f"Message ID: {message_id}")
                        print(f"Data: {self.teleop_data}, Type: {type(self.teleop_data)}")
                        # Update last_id to the current message ID
                        self.last_id = message_id
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


    def pub_timer_callback(self,event):
        if not(self.e) and self.teleop_mode :
            self.surge_pub.publish(Float64(5*self.data['surge']))
            self.sway_pub.publish(Float64(5*self.data['sway']))
            self.heave_pub.publish(Float64(self.depth))
            self.yaw_pub.publish(Float64(15*self.data['yaw']))
    
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
            # print("depth:",self.state["depth"])
            self.db.publish("vehicle-state",json.dumps(state_to_store))            
            # for stream_key, messages in response
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

            # print(self.state)
        except redis.exceptions.ConnectionError as e: 
            self.e = e
            print(f"Failed to connect to Redis: {e}")
        except Exception as e:
            self.e =e
            print(f"Error reading from stream: {e} \n {self.state}")

    def teleop_callback(self,event):
        self.teleop_read()
        if self.teleop_mode:         
            if self.teleop_data['value'] == "UP":
                self.heave_pub.publish(self.limit_val(self.state["depth"]-1,self.max_depth,0))
            elif self.teleop_data['value'] == "DOWN":
                self.heave_pub.publish(self.limit_val(self.state["depth"]+1,self.max_depth,0))
            elif self.teleop_data['value'] == "LEFT":
                self.sway_pub.publish(-0.5)
                rospy.sleep(rospy.Duration(2))
            elif self.teleop_data["value"] == "RIGHT":  # Handle "RIGHT" or other unexpected cases
                self.sway_pub.publish(0.5)
                rospy.sleep(rospy.Duration(2))

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
