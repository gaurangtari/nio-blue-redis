import argparse
import asyncio
import logging
import numpy as np
import gi
import json
import aiohttp
from aiortc import RTCPeerConnection, RTCSessionDescription, VideoStreamTrack, RTCIceCandidate
from av import VideoFrame
from gi.repository import Gst

logging.basicConfig(level=logging.INFO)

# Initialize GStreamer
Gst.init(None)

class Video:
    """GStreamer video capture class constructor"""

    def __init__(self, port=5600):
        self.port = port
        self._frame = None
        self.pipeline_started = False

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
        print(f"GStreamer command: {command}")
        self.video_pipe = Gst.parse_launch(command)
        self.video_pipe.set_state(Gst.State.PLAYING)

        # List all elements to ensure appsink is created
        print("Listing pipeline elements:")
        for element in self.video_pipe.iterate_elements():
            print(f"Element name: {element.get_name()}")

        self.video_sink = self.video_pipe.get_by_name('appsink0')
        if not self.video_sink:
            raise RuntimeError("Failed to get appsink from GStreamer pipeline")
        logging.info(f"(start gst) self.video_sink: {self.video_sink}")


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

class VideoStreamTrackFromGStreamer(VideoStreamTrack):
    def __init__(self, video_source):
        super().__init__()
        self.video_source = video_source
        self.video_source.run()

    async def recv(self):
        if self.video_source.frame_available():
            frame_data = self.video_source.frame()
            pts, time_base = await self.next_timestamp()
            frame = VideoFrame.from_ndarray(frame_data, format="bgr24")
            frame.pts = pts
            frame.time_base = time_base
            return frame
        else:
            await asyncio.sleep(0.1)

async def run(pc, video_source, signaling_url, role):
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(signaling_url) as ws:
            async def send_offer():
                offer = await pc.createOffer()
                await pc.setLocalDescription(offer)
                await ws.send_str(json.dumps({"type": "offer", "sdp": offer.sdp}))

            def add_tracks():
                pc.addTrack(VideoStreamTrackFromGStreamer(video_source))

            if role == "offer":
                add_tracks()
                await send_offer()

            async for msg in ws:
                data = json.loads(msg.data)
                if data["type"] == "answer":
                    await pc.setRemoteDescription(RTCSessionDescription(data["sdp"], "answer"))
                elif data["type"] == "offer":
                    await pc.setRemoteDescription(RTCSessionDescription(data["sdp"], "offer"))
                    add_tracks()
                    answer = await pc.createAnswer()
                    await pc.setLocalDescription(answer)
                    await ws.send_str(json.dumps({"type": "answer", "sdp": answer.sdp}))
                elif data["type"] == "candidate":
                    await pc.addIceCandidate(RTCIceCandidate(data["candidate"]))

async def main():
    parser = argparse.ArgumentParser(description="Stream RTP video over WebRTC")
    parser.add_argument("role", choices=["offer", "answer"])
    parser.add_argument("port", type=int, help="Port to receive RTP stream")
    parser.add_argument("--signaling-url", type=str, default="ws://localhost:8080/ws")
    args = parser.parse_args()

    signaling_url = args.signaling_url
    pc = RTCPeerConnection()
    video_source = Video(port=args.port)

    try:
        await run(pc=pc, video_source=video_source, signaling_url=signaling_url, role=args.role)
    except KeyboardInterrupt:
        pass
    finally:
        await pc.close()

if __name__ == "__main__":
    asyncio.run(main())
