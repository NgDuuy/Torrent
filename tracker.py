import http.server
import socketserver
import json
import threading
import time
import random
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
    def handle_get_file_hash(self, data):
        """Xử lý yêu cầu lấy hash từ tên file - Phiên bản đã sửa"""
        try:
            # Xử lý cả trường hợp data là string hoặc dict
            file_name = data if isinstance(data, str) else data.get("file_name", "")
            
            with self.lock:
                for file_hash, info in self.torrents.items():
                    if info["metadata"]["file_name"] == file_name:
                        return {
                            "status": "success",
                            "file_hash": file_hash,
                            "metadata": info["metadata"]
                        }
                return {"error": "File not found"}
        except Exception as e:
            return {"error": f"Error: {str(e)}"}

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
            
            active_peers = []
            piece_counts = [0] * len(self.torrents[file_hash]["metadata"]["pieces"])
            
            # Đếm số peer có mỗi piece
            for peer_info in self.torrents[file_hash]["peers"].values():
                if time.time() - peer_info["last_seen"] < 300:  # Peer active trong 5 phút
                    for i, has_piece in enumerate(peer_info["bitfield"]):
                        if has_piece:
                            piece_counts[i] += 1
            
            # Xác định các pieces hiếm (có ít peer nhất)
            min_count = min(piece_counts) if piece_counts else 0
            rare_pieces = [i for i, count in enumerate(piece_counts) if count == min_count]
            
            # Trả về thông tin peer kèm pieces hiếm
            for peer_id, peer_info in self.torrents[file_hash]["peers"].items():
                if time.time() - peer_info["last_seen"] < 300:
                    peer_data = {
                        "ip": peer_info["ip"],
                        "port": peer_info["port"],
                        "available_pieces": [i for i, has_piece in enumerate(peer_info["bitfield"]) if has_piece],
                        "rare_pieces": [i for i in rare_pieces if i in peer_info["bitfield"]],
                        "last_seen": peer_info["last_seen"],
                        "latency": random.uniform(0.1, 0.5),  # Giả lập latency
                        "peer_id": peer_id
                    }
                    active_peers.append(peer_data)
            
            return {
                "peers": active_peers,
                "piece_rarity": {i: count for i, count in enumerate(piece_counts)},
                "rare_pieces": rare_pieces
            }
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
    def handle_discover(self, data=None):
        """Xử lý yêu cầu discover - Phiên bản đã sửa"""
        with self.lock:
            files = []
            for file_hash, info in self.torrents.items():
                # Đếm số peer active (hoạt động trong 5 phút gần nhất)
                active_peers = sum(1 for peer in info["peers"].values() 
                            if time.time() - peer["last_seen"] < 300)
                
                files.append({
                    "file_name": info["metadata"]["file_name"],
                    "hash": file_hash,
                    "size": info["metadata"]["size"],
                    "active_peers": active_peers
                })
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
    def _set_headers(self, status=200):
        self.send_response(status)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

    def do_POST(self):
        try:
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data)
            
            print(f"\n[Tracker] Received request: {data.get('action')}")
            
            if not isinstance(data, dict) or "action" not in data:
                self._set_headers(400)
                self.wfile.write(json.dumps({"error": "Invalid request format"}).encode())
                return
                
            action = data["action"]
            handler_name = f"handle_{action}"
            
            if not hasattr(self.server.tracker, handler_name):
                self._set_headers(400)
                self.wfile.write(json.dumps({"error": "Invalid action"}).encode())
                return
                
            handler = getattr(self.server.tracker, handler_name)
            
            # Xử lý đặc biệt cho các hàm không cần data
            if action in ["discover"]:
                response = handler()
            else:
                response = handler(data)
            
            self._set_headers()
            self.wfile.write(json.dumps(response).encode())
            
        except json.JSONDecodeError:
            self._set_headers(400)
            self.wfile.write(json.dumps({"error": "Invalid JSON"}).encode())
        except Exception as e:
            print(f"[Tracker Error] {str(e)}")
            self._set_headers(500)
            self.wfile.write(json.dumps({"error": str(e)}).encode())

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