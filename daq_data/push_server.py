from flask import Flask, request, jsonify
from flask_cors import CORS
import threading
import time

app = Flask(__name__)
CORS(app)  # Allow Grafana (or any origin) to access the API

# Store the latest image and timestamp in memory (thread-safe)
latest = {"image": None, "timestamp": None}
lock = threading.Lock()

@app.route('/imagefeed', methods=['POST'])
def receive_image():
    data = request.json
    # Expected: {'image': 'data:image/png;base64,....', 'timestamp': 1234567890}
    if not data or 'image' not in data:
        return jsonify({"status": "error", "msg": "No image found"}), 400
    with lock:
        latest['image'] = data['image']
        latest['timestamp'] = data.get('timestamp', time.time())
    return jsonify({"status": "ok"})

@app.route('/imagefeed', methods=['GET'])
def serve_latest():
    with lock:
        if latest['image']:
            return jsonify({"image": latest['image'], "timestamp": latest['timestamp']})
        else:
            return jsonify({}), 204  # No content yet

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)