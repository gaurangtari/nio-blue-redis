#!/usr/bin/env python

import gi
import cv2
from cv_bridge import CvBridge, CvBridgeError
import numpy as np
from std_msgs.msg import Header
import rospy
import image_transport
gi.require_version('Gst', '1.0')
from gi.repository import Gst
from abc import ABCMeta, abstractmethod  # Abstract class.
from sensor_msgs.msg import Image
from message_filters import TimeSynchronizer, Subscriber
# Initialize GStreamer
Gst.init(None)


# Video configuration
margin_width = 50
caption_height = 60
font = cv2.FONT_HERSHEY_SIMPLEX
font_scale = 1
font_thickness = 2

class Video:
    """GStreamer video capture class constructor"""

    def __init__(self, port=5600):
        self.port = port
        self._frame = None

        # Video pipeline configuration
        self.video_source = f'udpsrc port={self.port}'
        self.video_codec = '! application/x-rtp, payload=96 ! rtph264depay ! h264parse ! avdec_h264'
        self.video_decode = '! decodebin ! videoconvert ! video/x-raw,format=(string)BGR ! videoconvert'
        self.video_sink_conf = '! appsink emit-signals=true sync=false max-buffers=2 drop=true'

        self.video_pipe = None
        self.video_sink = None

        self.run()

    def start_gst(self, config=None):
        if not config:
            config = [self.video_source, self.video_codec, self.video_decode, self.video_sink_conf]
        command = ' '.join(config)
        self.video_pipe = Gst.parse_launch(command)
        self.video_pipe.set_state(Gst.State.PLAYING)
        self.video_sink = self.video_pipe.get_by_name('appsink0')

    @staticmethod
    def gst_to_opencv(sample):
        buf = sample.get_buffer()
        caps = sample.get_caps()
        array = np.ndarray(
            (
                caps.get_structure(0).get_value('height'),
                caps.get_structure(0).get_value('width'),
                3
            ),
            buffer=buf.extract_dup(0, buf.get_size()), dtype=np.uint8)
        return array

    def frame(self):
        return self._frame

    def frame_available(self):
        return self._frame is not None

    def run(self):
        self.start_gst()
        self.video_sink.connect('new-sample', self.callback)

    def callback(self, sink):
        sample = sink.emit('pull-sample')
        new_frame = self.gst_to_opencv(sample)
        self._frame = new_frame
        return Gst.FlowReturn.OK

def create_header(frame_id):
    header = Header()
    header.stamp = rospy.Duration(1)   
    header.frame_id = frame_id
    # print(header)
    return header

class AbstractImagePublisher(object):
    __metaclass__ = ABCMeta

    def __init__(self, image_topic,
                 queue_size=10):
        self._pub = rospy.Publisher(image_topic, Image, queue_size=queue_size)
        self._cv_bridge = CvBridge()

    def publish(self, image, frame_id="head_camera"):
        self.ros_image = self._to_ros_image(image)
        self.ros_image.header = create_header(frame_id)
        self._pub.publish(self.ros_image)

    @abstractmethod
    def _to_ros_image(self, image):
        pass

class DepthImagePublisher(AbstractImagePublisher):

    def __init__(self, topic_name, queue_size=10):
        super(DepthImagePublisher, self).__init__(
            topic_name, queue_size)

    def _to_ros_image(self, cv2_uint16_image):

         # -- Check input.
        # image_16bit = (cv2_uint16_image.astype(np.uint16) * 256)
        image_8bit = cv2.normalize(cv2_uint16_image, None, 0, 255, cv2.NORM_MINMAX)
        image_8bit = image_8bit.astype(np.uint8) 
        # shape = image_16bit.shape  # (row, col)
        # assert(len(shape) == 2)
        # assert(type(image_16bit[0, 0] == np.uint16))

        # -- Convert to ROS format.
        ros_image = self._cv_bridge.cv2_to_imgmsg(image_8bit, "rgb8")
        return ros_image
    


    

if __name__ == '__main__':
    FRAME_ID = "head_camera" 
    rospy.init_node('depth_point_cloud_publisher')
    img_publisher = DepthImagePublisher("depth/image")
    # img_publisher2 = image_transport.Publisher("depth/image2")

    print("publisher creater")
    video = Video()

    rate = rospy.Rate(10)  # 10 Hz
    while not rospy.is_shutdown():
        if not video.frame_available():
            print("no video data")
            continue
        img2 = video.frame()
        print("image data",img2)
        if img2 is None or img2.size == 0:
            print("Error: No frame data.")
            continue
        
        else:
            raw_image = img2
            raw_image = cv2.resize(raw_image, (512, 512))
            image = cv2.cvtColor(raw_image, cv2.COLOR_BGR2RGB) / 255.0
            img_publisher.publish(image)
            # img_publisher2.publish(img_publisher.ros_image)
        rate.sleep()

        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

    video.video_pipe.set_state(Gst.State.NULL)
    cv2.destroyAllWindows()