import http.server
import socketserver
import json
import threading
import time
from http.server import BaseHTTPRequestHandler

class Tracker:
    def __init__(self, host='localhost', port=8000):
        self.host = host
        self.port = port
        self.torrents = {}
        self.file_name_to_hash = {}  # Map tên file tới hash
        self.lock = threading.Lock()
        print(f"Tracker initialized at {host}:{port}")

    def handle_share(self, data):
        with self.lock:
            file_hash = data['file_hash']
            if file_hash not in self.torrents:
                self.torrents[file_hash] = {
                    "metadata": {
                        "file_name": data['file_name'],
                        "pieces": data['pieces'],
                        "size": sum(p['size'] for p in data['pieces'])
                    },
                    "peers": {},
                    "piece_availability": [0] * len(data['pieces'])  # Theo dõi số peer có mỗi piece
                }
                # Thêm mapping tên file tới hash
                self.file_name_to_hash[data['file_name']] = file_hash
            
            # Cập nhật thông tin peer
            peer_info = {
                "ip": data.get('ip', '0.0.0.0'),
                "port": data['port'],
                "bitfield": [1] * len(data['pieces']),
                "last_seen": time.time()
            }
            self.torrents[file_hash]["peers"][data['peer_id']] = peer_info
            
            # Cập nhật độ phổ biến của các pieces
            for i in range(len(data['pieces'])):
                self.torrents[file_hash]["piece_availability"][i] = sum(
                    1 for p in self.torrents[file_hash]["peers"].values() if p["bitfield"][i]
                )
                
            return {"status": "success", "file_hash": file_hash}
    def handle_get_file_hash(self, file_name):
        with self.lock:
            return {"file_hash": self.file_name_to_hash.get(file_name)}
    def handle_get_metadata(self, data):
        file_hash = data["file_hash"]
        with self.lock:
            if file_hash not in self.torrents:
                return {"error": "File not found"}
            return {
                "status": "success",
                "metadata": self.torrents[file_hash]["metadata"]
            }
    def handle_get_peers(self, data):
        file_hash = data["file_hash"]
        with self.lock:
            if file_hash not in self.torrents:
                return {"peers": []}
            
            active_peers = [
                {"ip": peer["ip"], "port": peer["port"], 
                 "available_pieces": [i for i, has_piece in enumerate(peer["bitfield"]) if has_piece]}
                for peer in self.torrents[file_hash]["peers"].values()
            ]
            return {"peers": active_peers}
    def handle_get_piece_availability(self, data):
        file_hash = data["file_hash"]
        with self.lock:
            if file_hash not in self.torrents:
                return {"error": "File not found"}
            
            # Trả về số peer đang sở hữu mỗi piece
            piece_availability = []
            for piece_idx in range(len(self.torrents[file_hash]["pieces"])):
                count = sum(
                    1 for peer in self.torrents[file_hash]["peers"].values() 
                    if peer["bitfield"][piece_idx]
                )
                piece_availability.append(count)
            
            return {"piece_availability": piece_availability}
    def handle_discover(self):
        with self.lock:
            files = [{
                "file_name": info["metadata"]["file_name"],
                "hash": file_hash,
                "size": info["metadata"]["size"],
                "active_peers": len(info["peers"])
            } for file_hash, info in self.torrents.items()]
            return {"files": files}
    def handle_update_availability(self, data):
        """Cập nhật khi peer mới có piece"""
        file_hash = data["file_hash"]
        peer_id = data["peer_id"]
        piece_idx = data["piece_idx"]
        
        with self.lock:
            if file_hash in self.torrents and peer_id in self.torrents[file_hash]["peers"]:
                self.torrents[file_hash]["peers"][peer_id]["bitfield"][piece_idx] = 1
        return {"status": "success"}
class TrackerHandler(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

    def do_POST(self):
        try:
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data)
            
            print(f"\n[Tracker] Received request: {data.get('action')}")
            
            if data["action"] == "share":
                response = self.server.tracker.handle_share(data)
            elif data["action"] == "get_peers":
                response = self.server.tracker.handle_get_peers(data)
            elif data["action"] == "discover":
                response = self.server.tracker.handle_discover()
            elif data["action"] == "get_metadata":
                response = self.server.tracker.handle_get_metadata(data)
            elif data["action"] == "get_file_hash":
                response = self.server.tracker.handle_get_file_hash(data["file_name"])
            elif data["action"] == "update_availability":
                response = self.server.tracker.handle_update_availability(data)
            elif data["action"] == "get_piece_availability":
                response = self.server.tracker.handle_get_piece_availability(data)
            else:
                response = {"error": "Invalid action"}
            self._set_headers()
            self.wfile.write(json.dumps(response).encode())
            
        except Exception as e:
            print(f"[Tracker Error] {str(e)}")
            self.send_error(500, str(e))

class ThreadingHTTPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

if __name__ == "__main__":
    tracker = Tracker()
    server = ThreadingHTTPServer((tracker.host, tracker.port), TrackerHandler)
    server.tracker = tracker
    print(f"Tracker running on {tracker.host}:{tracker.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nTracker stopped")