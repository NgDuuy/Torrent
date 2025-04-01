import socket
import threading
import json
import time
import hashlib
import os
import random
import requests
import queue
import http.server
import socketserver
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, HTTPServer
class PeerHTTPHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    
    def do_GET(self):
        try:
            if self.path == '/ping':
                self.send_pong()
            elif self.path.startswith('/download_piece'):
                self.handle_download()
            else:
                self.send_error(404, "Not Found")
        except Exception as e:
            print(f"[HTTP Error] {str(e)}")
            self.send_error(500, str(e))
    
    def send_pong(self):
        """Phản hồi yêu cầu ping"""
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"PONG")
        print("[HTTP] Sent PONG response")
    
    def handle_download(self):
        """Xử lý yêu cầu download piece"""
        try:
            # Phân tích query parameters từ URL
            query = urllib.parse.urlparse(self.path).query
            params = urllib.parse.parse_qs(query)
            
            file_hash = params.get('file_hash', [''])[0]
            piece_idx = params.get('piece_idx', [''])[0]
            
            if not file_hash or not piece_idx:
                self.send_error(400, "Missing parameters")
                return

            piece_path = os.path.join(self.server.peer.repository, 
                                    f"{file_hash}_piece_{piece_idx}")
            
            if not os.path.exists(piece_path):
                self.send_error(404, "Piece not found")
                return

            # Gửi dữ liệu với chunked transfer encoding
            self.send_response(200)
            self.send_header('Content-type', 'application/octet-stream')
            self.send_header('Transfer-Encoding', 'chunked')
            self.end_headers()
            
            with open(piece_path, 'rb') as f:
                while True:
                    chunk = f.read(8192)
                    if not chunk:
                        break
                    self.wfile.write(f"{len(chunk):X}\r\n".encode())
                    self.wfile.write(chunk)
                    self.wfile.write(b"\r\n")
                self.wfile.write(b"0\r\n\r\n")
                
            print(f"[HTTP] Sent piece {piece_idx} successfully")
            
        except Exception as e:
            print(f"[Download Error] {str(e)}")
            self.send_error(500, str(e))
class EnhancedPeer:
    def __init__(self, tracker_host="localhost", tracker_port=8000, peer_port=8001):
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port
        self.peer_port = peer_port
        self.peer_id = hashlib.sha256(str(time.time()).encode()).hexdigest()[:20]
        self.piece_size = 512 * 1024  # 512KB
        self.repository = "./repository"
        self.active_downloads = {}
        self.shared_files = {}
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=10)  # Tăng số worker
        self.download_queue = queue.Queue()  # Hàng đợi download
        self.is_running = True
        self.peer_threads = []
        # Khởi tạo HTTP server
        class ThreadingHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
            timeout = 5  # Timeout cho các kết nối đến
            allow_reuse_address = True
            
        self.http_server = ThreadingHTTPServer(('0.0.0.0', self.peer_port), PeerHTTPHandler)
        self.http_server.peer = self
        threading.Thread(target=self.http_server.serve_forever, daemon=True).start()
        print(f"[PEER] HTTP server started on port {self.peer_port}")
        os.makedirs(self.repository, exist_ok=True)
        print(f"Peer ID: {self.peer_id}")
    def start(self):
        # Khởi động server socket
        server_thread = threading.Thread(target=self._run_server, daemon=True)
        server_thread.start()

        # Khởi động các worker xử lý download
        for _ in range(5):  # 5 luồng download đồng thời
            t = threading.Thread(target=self._download_worker, daemon=True)
            t.start()
            self.peer_threads.append(t)

        print(f"\nPeer server running on port {self.peer_port}")
        print("Commands: discover | share <file> | download <hash> | list | status | exit\n")
        self._command_interface()

    def _download_worker(self):
        """Luồng xử lý download từ hàng đợi"""
        while self.is_running:
            try:
                task = self.download_queue.get(timeout=1)
                if task:
                    file_hash, piece_idx, peer_ip, peer_port = task
                    self._download_piece_optimized(file_hash, piece_idx, peer_ip, peer_port)
                self.download_queue.task_done()
            except queue.Empty:
                continue

    def _run_server(self):
        """Khởi động HTTP server cho peer - Phiên bản mới"""
        server = HTTPServer(('0.0.0.0', self.peer_port), PeerHTTPHandler)
        server.peer = self
        print(f"[PEER SERVER] HTTP server running on port {self.peer_port}")
        server.serve_forever()

    def _handle_peer_connection(self, conn, addr):
        try:
            data = conn.recv(1024).decode().strip()
            
            if data == "PING":
                conn.sendall(b"PONG\n")
                return
            elif data.startswith("REQUEST"):
                _, file_hash, piece_idx = data.split()
                self._send_piece_with_logging(conn, file_hash, int(piece_idx))
                return
                
        except Exception as e:
            print(f"[PEER SERVER] Error with {addr}: {str(e)}")
        finally:
            conn.close()
    def _send_piece_with_logging(self, conn, file_hash, piece_idx):
        """Gửi piece với logging chi tiết"""
        try:
            piece_path = os.path.join(self.repository, f"{file_hash}_piece_{piece_idx}")
            if os.path.exists(piece_path):
                with open(piece_path, "rb") as f:
                    piece_data = f.read()
                    conn.sendall(piece_data)
                print(f"[UPLOAD] Sent piece {piece_idx} ({len(piece_data)} bytes)")
            else:
                conn.sendall(b"PIECE_NOT_FOUND")
                print(f"[UPLOAD] Piece {piece_idx} not found")
        except Exception as e:
            print(f"[UPLOAD] Error sending piece {piece_idx}: {str(e)}")
        finally:
            conn.close()
    def _send_piece(self, conn, file_hash, piece_idx):
        """Gửi mảnh dữ liệu cho peer yêu cầu"""
        piece_path = os.path.join(self.repository, f"{file_hash}_piece_{piece_idx}")
        if os.path.exists(piece_path):
            with open(piece_path, "rb") as f:
                conn.sendall(f.read())
            print(f"[+] Sent piece {piece_idx} of {file_hash}")
        else:
            conn.sendall(b"PIECE_NOT_FOUND")

    def _command_interface(self):
        """Giao diện điều khiển với khả năng xử lý đồng thời - Phiên bản đã sửa"""
        while self.is_running:
            try:
                cmd = input("Peer> ").strip().split()
                if not cmd:
                    continue
                    
                if cmd[0] == "discover":
                    threading.Thread(target=self._handle_discover, daemon=True).start()
                elif cmd[0] == "share" and len(cmd) > 1:
                    threading.Thread(target=self._share_file, args=(cmd[1],), daemon=True).start()
                elif cmd[0] == "download" and len(cmd) > 1:
                    resume = len(cmd) > 2 and cmd[2] == "--resume"
                    # Xác định xem là tên file hay hash
                    if any(c in cmd[1] for c in [":", "/", "\\", "."]):  # Giả sử đây là tên file
                        threading.Thread(
                            target=self._download_file_by_name,
                            args=(cmd[1], resume),
                            daemon=True
                        ).start()
                    else:  # Nếu không phải tên file thì coi như là hash
                        threading.Thread(
                            target=self._download_file,
                            args=(cmd[1], resume),
                            daemon=True
                        ).start()
                elif cmd[0] == "list":
                    self._list_shared_files()
                elif cmd[0] == "status":
                    self._check_download_status()
                elif cmd[0] == "exit":
                    self._graceful_exit()
                    break
                else:
                    print("Unknown command. Available commands:")
                    print("discover | share <file> | download <file_name_or_hash> | list | status | exit")
            except Exception as e:
                print(f"Command error: {str(e)}")
    def _send_to_tracker(self, data):
        """Phiên bản cải tiến với logging chi tiết"""
        print(f"[DEBUG] Sending to tracker: {json.dumps(data, indent=2)}")
        
        max_retries = 3
        base_timeout = 5
        session = requests.Session()
        
        for attempt in range(max_retries):
            try:
                response = session.post(
                    f"http://{self.tracker_host}:{self.tracker_port}",
                    json=data,
                    headers={'Content-Type': 'application/json'},
                    timeout=base_timeout * (attempt + 1)
                )
                
                print(f"[DEBUG] Tracker response: {response.status_code}, {response.text}")
                
                if response.status_code != 200:
                    print(f"[TRACKER] HTTP Error {response.status_code}")
                    continue
                    
                return response.json()
                
            except requests.exceptions.RequestException as e:
                print(f"[TRACKER] Attempt {attempt+1} failed: {type(e).__name__}: {str(e)}")
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1)
                    print(f"[TRACKER] Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
            except Exception as e:
                print(f"[TRACKER] Unexpected error: {str(e)}")
                break
            finally:
                session.close()
        
        print("[TRACKER] All attempts failed")
        return None

    def _share_file(self, file_path):
        """Chia sẻ file lên tracker với kiểm tra hash và xử lý lỗi chi tiết"""
        try:
            # Kiểm tra file tồn tại
            full_path = os.path.abspath(file_path)
            if not os.path.isfile(full_path):
                print(f"[-] File not found: {full_path}")
                return False

            print("[+] Calculating file hash...")
            file_hash = self._calculate_file_hash(full_path)
            print(f"[+] File hash: {file_hash}")

            print("[+] Splitting file into pieces...")
            pieces = self._split_file(full_path, file_hash)
            
            # KIỂM TRA HASH TỪNG PIECE
            print("[+] Verifying pieces integrity...")
            for piece in pieces:
                piece_path = os.path.join(self.repository, f"{file_hash}_piece_{piece['index']}")
                if not os.path.exists(piece_path):
                    print(f"[-] Missing piece {piece['index']} at {piece_path}")
                    return False
                    
                with open(piece_path, 'rb') as f:
                    actual_hash = hashlib.sha1(f.read()).hexdigest()
                    if actual_hash != piece['hash']:
                        print(f"[-] Hash mismatch for piece {piece['index']}")
                        print(f"     Expected: {piece['hash']}")
                        print(f"     Actual:   {actual_hash}")
                        return False

            # Tạo metadata với thông tin đầy đủ
            metadata = {
                "file_name": os.path.basename(full_path),
                "file_size": os.path.getsize(full_path),
                "piece_size": self.piece_size,
                "pieces": pieces,
                "file_hash": file_hash,
                "created_at": time.time()
            }
            
            # Lưu metadata vào file
            metadata_path = os.path.join(self.repository, f"{file_hash}_metadata.json")
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            print(f"[+] Saved metadata to {metadata_path}")

            # Gửi thông tin chia sẻ lên tracker
            tracker_data = {
                "action": "share",
                "file_hash": file_hash,
                "file_name": metadata["file_name"],
                "pieces": pieces,
                "piece_availability": [1] * len(pieces),
                "peer_id": self.peer_id,
                "port": self.peer_port,
                "ip": self._get_local_ip(),
                "timestamp": int(time.time())
            }

            print("[+] Registering file with tracker...")
            response = self._send_to_tracker(tracker_data)
            
            if response and response.get("status") == "success":
                print(f"[+] Shared successfully! File hash: {file_hash}")
                with self.lock:
                    self.shared_files[file_hash] = {
                        "file_name": metadata["file_name"],
                        "pieces": pieces,
                        "size": metadata["file_size"],
                        "last_updated": time.time()
                    }
                return True
            
            print("[-] Failed to share file with tracker")
            if response:
                print(f"     Tracker response: {response}")
            return False
            
        except Exception as e:
            print(f"[-] Share failed with error: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    def _get_metadata(self, file_hash):
        """Lấy metadata từ tracker"""
        resp = self._send_to_tracker({
            "action": "get_metadata",
            "file_hash": file_hash
        })
        return resp.get("metadata") if resp else None
    def _get_peers(self, file_hash):
        """Lấy danh sách peers có sẵn, sắp xếp theo latency"""
        resp = self._send_to_tracker({
            "action": "get_peers",
            "file_hash": file_hash
        })
        if not resp or "peers" not in resp:
            return []
        
        # Thêm thông tin giả lập latency (trong thực tế dùng ping)
        for p in resp["peers"]:
            p["latency"] = random.uniform(0.1, 0.5)  # Giả lập 100-500ms
            
        return sorted(resp["peers"], key=lambda x: x["latency"])

    def _get_needed_pieces(self, file_hash, pieces_info):
        """Xác định các pieces còn thiếu"""
        existing = {
            int(f.split('_')[-1])
            for f in os.listdir(self.repository)
            if f.startswith(f"{file_hash}_piece_")
        }
        return [p["index"] for p in pieces_info if p["index"] not in existing]
    def _download_file_by_name(self, file_name, resume=False):
        """Tải file bằng tên thay vì hash - Phiên bản đã sửa"""
        print(f"[DOWNLOAD] Requesting hash for file: {file_name}")
        
        response = self._send_to_tracker({
            "action": "get_file_hash",
            "file_name": file_name
        })
        
        if not response:
            print("[-] No response from tracker when getting file hash")
            return False
            
        if "error" in response:
            print(f"[-] Tracker error: {response['error']}")
            return False
            
        if "file_hash" not in response:
            print("[-] Invalid tracker response: missing file_hash")
            return False
            
        file_hash = response["file_hash"]
        print(f"[DOWNLOAD] Received hash: {file_hash} for file: {file_name}")
        return self._download_file(file_hash, resume)

    def _get_rarest_pieces(self, file_hash, needed_pieces, metadata):
        """Sắp xếp các pieces theo độ hiếm (rarest first)"""
        response = self._send_to_tracker({
            "action": "get_piece_availability",
            "file_hash": file_hash
        })
        
        if not response or "piece_availability" not in response:
            return needed_pieces  # Fallback về thứ tự ban đầu nếu không lấy được thông tin
        
        # Sắp xếp các pieces cần tải theo độ phổ biến (hiếm nhất trước)
        piece_availability = response["piece_availability"]
        return sorted(
            needed_pieces,
            key=lambda x: piece_availability[x]
        )
    def _get_rarest_pieces_first(self, file_hash, needed_pieces):
        """Lấy danh sách pieces sắp xếp theo độ hiếm"""
        response = self._send_to_tracker({
            "action": "get_piece_availability",
            "file_hash": file_hash
        })
        
        if not response or "piece_availability" not in response:
            return needed_pieces  # Fallback về thứ tự ban đầu
        
        availability = response["piece_availability"]
        
        # Sắp xếp các pieces cần tải theo độ phổ biến (hiếm nhất trước)
        return sorted(
            needed_pieces,
            key=lambda x: availability[x] if x < len(availability) else 0
        )

    def _select_optimal_peer(self, piece_idx, peers):
        """Chọn peer tối ưu dựa trên:
        - Có piece cần tải
        - Độ hiếm của các piece mà peer có
        - Latency thấp
        """
        candidates = [p for p in peers if piece_idx in p.get("available_pieces", [])]
        
        if not candidates:
            return None
        
        # Tính điểm cho từng peer
        def calculate_score(peer):
            # Điểm latency (càng thấp càng tốt)
            latency_score = 1.0 - min(peer.get("latency", 1.0), 1.0)
            
            # Điểm độ hiếm (peer có càng nhiều piece hiếm càng tốt)
            rarity_score = peer.get("rare_pieces_count", 0) / 10.0  # Chuẩn hóa
            
            # Trọng số: 60% latency, 40% độ hiếm
            return 0.6 * latency_score + 0.4 * rarity_score
        
        # Chọn peer có điểm cao nhất
        return max(candidates, key=lambda p: calculate_score(p))
    def _check_peer_connection(self, ip, port):
        """Kiểm tra kết nối peer bằng HTTP ping"""
        try:
            response = requests.get(
                f"http://{ip}:{port}/ping",
                timeout=2
            )
            if response.status_code == 200 and response.text.strip() == "PONG":
                return True
            print(f"[CONNECTION] Invalid ping response from {ip}:{port}")
        except Exception as e:
            print(f"[CONNECTION] Error pinging {ip}:{port}: {str(e)}")
        return False
    def _parallel_download(self, file_hash, needed_pieces, peers):
        """Phiên bản đã thêm kiểm tra kết nối"""
        rarest_pieces = self._get_rarest_pieces_first(file_hash, needed_pieces)
        
        for piece_idx in rarest_pieces:
            peer = self._select_optimal_peer(piece_idx, peers)
            if peer and self._check_peer_connection(peer["ip"], peer["port"]):
                self.download_queue.put((file_hash, piece_idx, peer["ip"], peer["port"]))
            else:
                print(f"[DOWNLOAD] No available peer for piece {piece_idx}")
        
        self.download_queue.join()
        return self._check_complete(file_hash)
    def _select_peer_for_piece(self, piece_idx, peers):
        """Chọn peer có piece với latency thấp nhất và ưu tiên peer có nhiều pieces hiếm"""
        candidates = [
            p for p in peers 
            if piece_idx in p.get("available_pieces", [])
        ]
        
        if not candidates:
            return None
        
        # Tính điểm cho mỗi peer dựa trên latency và số pieces hiếm mà họ có
        def peer_score(peer):
            # Latency càng thấp càng tốt (chiếm 60% điểm)
            latency_score = (1 - peer["latency"]/0.5) * 0.6  # Giả sử latency max là 0.5s
            
            # Số pieces hiếm mà peer có (chiếm 40% điểm)
            rare_pieces_score = (len(peer.get("rare_pieces", [])) / 10 * 0.4 ) # Chuẩn hóa về 0-0.4
            
            return latency_score + rare_pieces_score
        
        # Tính điểm cho từng candidate
        scored_peers = [(peer, peer_score(peer)) for peer in candidates]
        
        # Chọn peer có điểm cao nhất
        best_peer = max(scored_peers, key=lambda x: x[1])[0]
        return best_peer
    def _get_existing_pieces(self, file_hash):
        """Lấy danh sách các pieces đã tải về"""
        pieces = set()
        for f in os.listdir(self.repository):
            if f.startswith(f"{file_hash}_piece_"):
                try:
                    piece_idx = int(f.split('_')[-1])
                    pieces.add(piece_idx)
                except ValueError:
                    continue
        return pieces
    def _update_piece_availability(self, file_hash, piece_idx):
        """Cập nhật độ phổ biến của piece sau khi tải xong"""
        self._send_to_tracker({
            "action": "update_availability",
            "file_hash": file_hash,
            "piece_idx": piece_idx,
            "peer_id": self.peer_id
        })

    def _download_piece_optimized(self, file_hash, piece_idx, peer_ip, peer_port):
        """Tải piece với kiểm tra hash và retry"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"[DOWNLOAD] Attempt {attempt+1} for piece {piece_idx} from {peer_ip}:{peer_port}")
                
                url = f"http://{peer_ip}:{peer_port}/download_piece?file_hash={file_hash}&piece_idx={piece_idx}"
                
                with requests.Session() as session:
                    # Tăng timeout và thêm retry
                    session.mount('http://', requests.adapters.HTTPAdapter(
                        max_retries=3,
                        pool_connections=1,
                        pool_maxsize=1
                    ))
                    
                    response = session.get(url, stream=True, timeout=5)
                    
                    if response.status_code != 200:
                        print(f"[DOWNLOAD] HTTP Error {response.status_code}")
                        continue
                        
                    # Lưu tạm file để kiểm tra hash
                    temp_path = os.path.join(self.repository, f"{file_hash}_piece_{piece_idx}.tmp")
                    with open(temp_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    
                    # Kiểm tra hash
                    expected_hash = self._get_expected_piece_hash(file_hash, piece_idx)
                    if expected_hash:
                        downloaded_hash = hashlib.sha1(open(temp_path, 'rb').read()).hexdigest()
                        if downloaded_hash != expected_hash:
                            print(f"[HASH] Mismatch: expected {expected_hash}, got {downloaded_hash}")
                            os.remove(temp_path)
                            continue
                    
                    # Xác nhận download thành công
                    final_path = os.path.join(self.repository, f"{file_hash}_piece_{piece_idx}")
                    os.replace(temp_path, final_path)
                    return True
                    
            except requests.exceptions.RequestException as e:
                print(f"[DOWNLOAD] Request failed: {type(e).__name__}: {str(e)}")
            except Exception as e:
                print(f"[DOWNLOAD] Unexpected error: {str(e)}")
            
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                print(f"[DOWNLOAD] Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
        
        print(f"[DOWNLOAD] Failed to download piece {piece_idx} after {max_retries} attempts")
        return False
    def _get_expected_piece_hash(self, file_hash, piece_idx):
        """Lấy hash mong đợi của piece từ metadata"""
        metadata_path = os.path.join(self.repository, f"{file_hash}_metadata.json")
        if not os.path.exists(metadata_path):
            return None
            
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
            for piece in metadata['pieces']:
                if piece['index'] == piece_idx:
                    return piece['hash']
        return None
    def _get_expected_piece_size(self, file_hash, piece_idx):
        """Lấy kích thước dự kiến của piece từ metadata"""
        metadata_path = os.path.join(self.repository, f"{file_hash}_metadata.json")
        if not os.path.exists(metadata_path):
            return None
            
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
            for piece in metadata['pieces']:
                if piece['index'] == piece_idx:
                    return piece['size']
        return None
    def _save_download_state(self):
        """Lưu trạng thái download vào file"""
        state = {
            "active_downloads": self.active_downloads,
            "shared_files": self.shared_files
        }
        with open(os.path.join(self.repository, "download_state.json"), "w") as f:
            json.dump(state, f)

    def _load_download_state(self):
        """Tải trạng thái download từ file"""
        state_path = os.path.join(self.repository, "download_state.json")
        if os.path.exists(state_path):
            with open(state_path, "r") as f:
                state = json.load(f)
                self.active_downloads = state.get("active_downloads", {})
                self.shared_files = state.get("shared_files", {})
    def _download_file(self, file_hash, resume=False):
        """Tải file với chiến lược rarest-first"""
        try:
            # Lấy metadata và lưu lại
            metadata = self._get_metadata(file_hash)
            if not metadata:
                print("[-] Failed to get file metadata from tracker")
                return False
            # Lưu metadata ngay khi nhận được
            metadata_path = os.path.join(self.repository, f"{file_hash}_metadata.json")
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f)
            print(f"[+] Saved metadata to {metadata_path}")
            # Khởi tạo thông tin download
            with self.lock:
                if file_hash not in self.active_downloads:
                    self.active_downloads[file_hash] = {
                        "file_name": metadata["file_name"],
                        "total_pieces": len(metadata["pieces"]),
                        "downloaded": set(),
                        "status": "downloading"
                    }

            # Lấy danh sách peers từ tracker (kèm thông tin độ hiếm)
            peers_response = self._send_to_tracker({
                "action": "get_peers",
                "file_hash": file_hash
            })
            
            if not peers_response or "peers" not in peers_response:
                print("[-] No peers available for this file")
                return False
            
            # Xác định pieces cần tải
            needed_pieces = [
                p["index"] for p in metadata["pieces"] 
                if p["index"] not in self.active_downloads[file_hash]["downloaded"]
            ]
            
            # Thực hiện download với rarest-first strategy
            success = self._parallel_download_rarest_first(
                file_hash,
                needed_pieces,
                peers_response["peers"]
            )
            
            if success:
                return self._reconstruct_file(file_hash)
            return False
            
        except Exception as e:
            print(f"[-] Download error: {str(e)}")
            return False
    def _parallel_download_rarest_first(self, file_hash, needed_pieces, peers):
        """Tải song song với ưu tiên pieces hiếm nhất trước"""
        if not peers:
            print("[-] No available peers for download")
            return False

        # Lấy thông tin độ hiếm từ tracker
        tracker_info = self._send_to_tracker({
            "action": "get_peers",
            "file_hash": file_hash
        })
        
        if not tracker_info or "piece_rarity" not in tracker_info:
            print("[-] Could not get piece rarity info")
            return False
        
        # Sắp xếp pieces theo độ hiếm (hiếm nhất trước)
        pieces_sorted = sorted(
            needed_pieces,
            key=lambda x: tracker_info["piece_rarity"].get(x, float('inf')))
        
        # Tạo task download cho từng piece
        futures = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            for piece_idx in pieces_sorted:
                # Chọn peer tối ưu cho piece này
                best_peer = self._select_optimal_peer(piece_idx, peers)
                
                if best_peer:
                    futures.append(executor.submit(
                        self._download_piece_with_retry,
                        file_hash, piece_idx, best_peer["ip"], best_peer["port"]
                    ))
                else:
                    print(f"[!] No peer available for piece {piece_idx}")

            # Đợi tất cả task hoàn thành
            for future in as_completed(futures):
                if not future.result():
                    print("[!] Some pieces failed to download")

        return self._check_complete(file_hash)
    def _download_piece_with_retry(self, file_hash, piece_idx, peer_ip, peer_port):
        for attempt in range(3):
            try:
                url = f"http://{peer_ip}:{peer_port}/download_piece?file_hash={file_hash}&piece_idx={piece_idx}"
                response = requests.get(url, timeout=5, stream=True)
                
                if response.status_code == 200:
                    # Lưu tạm file để kiểm tra hash
                    temp_path = f"{file_hash}_piece_{piece_idx}.tmp"
                    with open(temp_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    
                    # Kiểm tra hash
                    if self._verify_piece_hash(file_hash, piece_idx, temp_path):
                        final_path = f"{file_hash}_piece_{piece_idx}"
                        os.replace(temp_path, final_path)
                        return True
                    os.remove(temp_path)
            except Exception as e:
                print(f"[DOWNLOAD] Attempt {attempt+1} failed: {str(e)}")
        return False
    def _verify_piece_hash(self, file_hash, piece_idx, piece_path):
        """Kiểm tra hash piece với metadata"""
        metadata = self._get_metadata(file_hash)
        expected_hash = next(p['hash'] for p in metadata['pieces'] if p['index'] == piece_idx)
        
        with open(piece_path, 'rb') as f:
            actual_hash = hashlib.sha1(f.read()).hexdigest()
        
        if actual_hash != expected_hash:
            print(f"[HASH] Mismatch for piece {piece_idx}: expected {expected_hash}, got {actual_hash}")
            return False
        return True
    def _select_best_peer_for_piece(self, piece_idx, peers):
        """Chọn peer tốt nhất để tải piece cụ thể"""
        candidates = [p for p in peers if piece_idx in p.get("available_pieces", [])]
        
        if not candidates:
            return None
        
        # Ưu tiên peer có latency thấp và hoạt động gần đây
        def peer_score(peer):
            # Độ mới (càng gần current time càng tốt)
            recency = (time.time() - peer.get("last_seen", 0)) / 300  # Chuẩn hóa về 0-1
            
            # Độ tin cậy (giả sử peer có ít pieces hiếm hơn sẽ ổn định hơn)
            rare_pieces = len([p for p in peer.get("available_pieces", []) 
                            if p in self._get_rare_pieces()])
            reliability = 1 - (rare_pieces / len(peer.get("available_pieces", []))) if peer.get("available_pieces") else 0.5
            
            return 0.6 * (1 - recency) + 0.4 * reliability
        
        return max(candidates, key=peer_score)

    def _get_rare_pieces(self):
        """Xác định các pieces hiếm (có ít peer sở hữu nhất)"""
        # Trong thực tế, cần lấy từ tracker
        return set()  # Tạm thời trả về set rỗng
    def _download_piece(self, file_hash, piece_idx, peer_ip, peer_port):
        """Tải một mảnh cụ thể từ peer"""
        try:
            with socket.create_connection((peer_ip, peer_port), timeout=5) as s:
                s.sendall(f"REQUEST {file_hash} {piece_idx}".encode())
                data = s.recv(self.piece_size + 1024)
                
                if data == b"PIECE_NOT_FOUND":
                    print(f"[-] Piece {piece_idx} not found on {peer_ip}:{peer_port}")
                    return False
                
                piece_path = os.path.join(self.repository, f"{file_hash}_piece_{piece_idx}")
                with open(piece_path, "wb") as f:
                    f.write(data)
                
                with self.lock:
                    self.active_downloads[file_hash]["downloaded"].add(piece_idx)
                
                print(f"[+] Downloaded piece {piece_idx} from {peer_ip}:{peer_port}")
                return True
        except Exception as e:
            print(f"[-] Error downloading piece {piece_idx}: {str(e)}")
            return False

    def _check_complete(self, file_hash):
        """Kiểm tra đã tải đủ tất cả mảnh chưa"""
        with self.lock:
            if file_hash not in self.active_downloads:
                return False
            return len(self.active_downloads[file_hash]["downloaded"]) == self.active_downloads[file_hash]["total_pieces"]

    def _reconstruct_file(self, file_hash):
        """Ghép file với kiểm tra hash toàn vẹn"""
        metadata_path = os.path.join(self.repository, f"{file_hash}_metadata.json")
        if not os.path.exists(metadata_path):
            print("[-] Metadata not found")
            return False
            
        with open(metadata_path) as f:
            metadata = json.load(f)
        
        # Kiểm tra tất cả pieces trước khi ghép
        for piece in metadata['pieces']:
            piece_path = os.path.join(self.repository, f"{file_hash}_piece_{piece['index']}")
            if not os.path.exists(piece_path):
                print(f"[-] Missing piece {piece['index']}")
                return False
                
            with open(piece_path, 'rb') as f:
                piece_hash = hashlib.sha1(f.read()).hexdigest()
                if piece_hash != piece['hash']:
                    print(f"[-] Hash mismatch in piece {piece['index']}")
                    return False
        
        # Ghép file
        output_path = os.path.join(self.repository, metadata["file_name"])
        with open(output_path, 'wb') as out_file:
            for piece in sorted(metadata['pieces'], key=lambda x: x['index']):
                piece_path = os.path.join(self.repository, f"{file_hash}_piece_{piece['index']}")
                with open(piece_path, 'rb') as piece_file:
                    out_file.write(piece_file.read())
        
        print(f"[+] File reconstructed successfully: {output_path}")
        return True
    def _cleanup_pieces(self, file_hash):
        """Dọn dẹp các pieces sau khi ghép file thành công"""
        # Chỉ xóa pieces nếu file đã được ghép thành công
        output_file = os.path.join(self.repository, f"{file_hash}_complete")
        if os.path.exists(output_file):
            for f in os.listdir(self.repository):
                if f.startswith(f"{file_hash}_piece_"):
                    try:
                        os.remove(os.path.join(self.repository, f))
                    except:
                        pass
        # Giữ lại metadata file để có thể seed sau này
        with self.lock:
            if file_hash in self.active_downloads:
                del self.active_downloads[file_hash]
            self._save_download_state()
    def _handle_discover(self):
        """Lấy danh sách file từ tracker - Phiên bản đã sửa"""
        try:
            response = self._send_to_tracker({"action": "discover"})
            
            if not response:
                print("[-] No response from tracker")
                return
                
            if "error" in response:
                print(f"[-] Tracker error: {response['error']}")
                return
                
            if "files" not in response:
                print("[-] Invalid response format: missing 'files' field")
                return
                
            print("\nAvailable files:")
            for file in response["files"]:
                print(f"- {file['file_name']} (Hash: {file['hash'][:8]}...) [Peers: {file.get('active_peers', 0)}]")
                
        except Exception as e:
            print(f"[-] Discover error: {str(e)}")
    def _list_shared_files(self):
        """Liệt kê các file đang chia sẻ (cả local và từ tracker)"""
        # Lấy danh sách file từ tracker
        tracker_files = self._send_to_tracker({"action": "discover"})
        
        print("\nShared files:")
        print("=== Local shared files ===")
        if not self.shared_files:
            print("No local files are being shared")
        else:
            for file_hash, file_name in self.shared_files.items():
                print(f"- {file_name} (Hash: {file_hash[:8]}...)")
        
        if tracker_files and "files" in tracker_files:
            print("\n=== Available files on tracker ===")
            for file in tracker_files["files"]:
                print(f"- {file['file_name']} (Hash: {file['hash'][:8]}...) [Peers: {file.get('active_peers', 0)}]")
    def _check_download_status(self):
        """Hiển thị trạng thái download"""
        if not self.active_downloads:
            print("No active downloads")
            return
        
        print("\nDownload status:")
        for file_hash, status in self.active_downloads.items():
            print(f"- {file_hash[:8]}...: {len(status['downloaded'])}/{status['total_pieces']} pieces")

    def _graceful_exit(self):
        """Thoát chương trình an toàn"""
        print("\n[+] Shutting down peer...")
        self.is_running = False
        
        # Dừng các worker thread
        for _ in range(len(self.peer_threads)):
            self.download_queue.put(None)
        
        self._save_download_state()
        self.executor.shutdown()
        print("[+] Peer stopped gracefully")

    def _calculate_file_hash(self, file_path):
        """Tính toán hash SHA-256 của file"""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _split_file(self, file_path, file_hash):
        """Chia file thành các mảnh"""
        pieces = []
        try:
            with open(file_path, "rb") as f:
                i = 0
                while True:
                    piece_data = f.read(self.piece_size)
                    if not piece_data:
                        break
                    
                    piece_hash = hashlib.sha1(piece_data).hexdigest()
                    piece_path = os.path.join(self.repository, f"{file_hash}_piece_{i}")
                    
                    with open(piece_path, "wb") as p:
                        p.write(piece_data)
                    
                    pieces.append({
                        "index": i,
                        "hash": piece_hash,
                        "size": len(piece_data)
                    })
                    i += 1
            
            # Lưu metadata file
            metadata = {
                "file_name": os.path.basename(file_path),
                "file_size": os.path.getsize(file_path),
                "piece_size": self.piece_size,
                "pieces": pieces,
                "file_hash": file_hash
            }
            with open(os.path.join(self.repository, f"{file_hash}_metadata.json"), "w") as f:
                json.dump(metadata, f)
            
            return pieces
        except Exception as e:
            print(f"[-] Error splitting file: {str(e)}")
            return []

    def _get_local_ip(self):
        """Lấy địa chỉ IP local"""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except:
            return "127.0.0.1"

if __name__ == "__main__":
    try:
        peer = EnhancedPeer(peer_port=8001)
        peer.start()
    except KeyboardInterrupt:
        print("\n[!] Peer stopped by user")
    except Exception as e:
        print(f"[!] Fatal error: {str(e)}")