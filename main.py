from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

app = FastAPI()

# Connection manager to keep track of active viewer devices
class ConnectionManager:
    def __init__(self):
        self.active_viewers: list[WebSocket] = []

    async def connect_viewer(self, websocket: WebSocket):
        await websocket.accept()
        self.active_viewers.append(websocket)

    def disconnect_viewer(self, websocket: WebSocket):
        if websocket in self.active_viewers:
            self.active_viewers.remove(websocket)

    async def broadcast_frame(self, frame_data: str):
        disconnected = []
        for viewer in self.active_viewers:
            try:
                await viewer.send_text(frame_data)
            except Exception:
                disconnected.append(viewer)
        for viewer in disconnected:
            self.disconnect_viewer(viewer)

manager = ConnectionManager()

# HTML for Mobile 1 (Camera Unit)
CAMERA_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>CCTV Camera Unit</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body { font-family: sans-serif; text-align: center; background: #121212; color: #fff; margin: 0; padding: 20px; }
        video { width: 100%; max-width: 400px; border-radius: 10px; margin-top: 10px; }
        .status { padding: 8px 16px; border-radius: 20px; font-weight: bold; display: inline-block; margin-bottom: 10px; }
        .connected { background: #2e7d32; }
        .disconnected { background: #c62828; }
        button { padding: 12px 24px; font-size: 16px; border: none; border-radius: 8px; background: #007bff; color: white; cursor: pointer; }
    </style>
</head>
<body>
    <h2>CCTV Streamer (Mobile 1)</h2>
    <div id="status" class="status disconnected">Disconnected</div>
    <br>
    <button onclick="startStreaming()">Start Camera & Stream</button>
    <br>
    <video id="video" autoplay playsinline muted></video>
    <canvas id="canvas" style="display:none;"></canvas>

    <script>
        let ws;
        const video = document.getElementById('video');
        const canvas = document.getElementById('canvas');
        const ctx = canvas.getContext('2d');
        const statusDiv = document.getElementById('status');

        async function startStreaming() {
            try {
                // Requests rear camera by default
                const stream = await navigator.mediaDevices.getUserMedia({
                    video: { facingMode: "environment", width: { max: 640 }, height: { max: 480 } },
                    audio: false
                });
                video.srcObject = stream;

                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                ws = new WebSocket(`${protocol}//${window.location.host}/ws/stream`);

                ws.onopen = () => {
                    statusDiv.innerText = "Streaming Live";
                    statusDiv.className = "status connected";
                    setInterval(sendFrame, 100); // Sends ~10 frames per second
                };

                ws.onclose = () => {
                    statusDiv.innerText = "Disconnected";
                    statusDiv.className = "status disconnected";
                };
            } catch (err) {
                alert("Camera access error: " + err.message);
            }
        }

        function sendFrame() {
            if (ws && ws.readyState === WebSocket.OPEN) {
                canvas.width = video.videoWidth || 320;
                canvas.height = video.videoHeight || 240;
                ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
                const dataUrl = canvas.toDataURL('image/jpeg', 0.5); // Compression quality 50%
                ws.send(dataUrl);
            }
        }
    </script>
</body>
</html>
"""

# HTML for Mobile 2 (Viewer / Monitor)
VIEWER_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>CCTV Monitor</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body { font-family: sans-serif; text-align: center; background: #121212; color: #fff; margin: 0; padding: 20px; }
        img { width: 100%; max-width: 600px; border-radius: 10px; border: 2px solid #333; margin-top: 10px; background: #000; }
        .status { padding: 8px 16px; border-radius: 20px; font-weight: bold; display: inline-block; margin-bottom: 10px; }
        .live { background: #c62828; animation: blink 1.5s infinite; }
        .offline { background: #616161; }
        @keyframes blink { 50% { opacity: 0.5; } }
    </style>
</head>
<body>
    <h2>CCTV Monitor (Mobile 2)</h2>
    <div id="status" class="status offline">Connecting...</div>
    <br>
    <img id="feed" src="" alt="Waiting for live camera feed...">

    <script>
        const statusDiv = document.getElementById('status');
        const feedImg = document.getElementById('feed');

        function connectViewer() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const ws = new WebSocket(`${protocol}//${window.location.host}/ws/viewer`);

            ws.onopen = () => {
                statusDiv.innerText = "LIVE FEED";
                statusDiv.className = "status live";
            };

            ws.onmessage = (event) => {
                feedImg.src = event.data;
            };

            ws.onclose = () => {
                statusDiv.innerText = "Stream Offline - Reconnecting...";
                statusDiv.className = "status offline";
                setTimeout(connectViewer, 2000);
            };
        }

        connectViewer();
    </script>
</body>
</html>
"""

@app.get("/camera", response_class=HTMLResponse)
async function camera_page():
    return CAMERA_HTML

@app.get("/", response_class=HTMLResponse)
@app.get("/viewer", response_class=HTMLResponse)
async function viewer_page():
    return VIEWER_HTML

@app.websocket("/ws/stream")
async function websocket_stream(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            frame_data = await websocket.receive_text()
            await manager.broadcast_frame(frame_data)
    except WebSocketDisconnect:
        pass

@app.websocket("/ws/viewer")
async function websocket_viewer(websocket: WebSocket):
    await manager.connect_viewer(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect_viewer(websocket)
      
