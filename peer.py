import socket
import threading
import json
import time
import hashlib
import os
import random
import requests
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, HTTPServer
class PeerHTTPHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data)
        
        if self.path == '/download':
            self.server.peer._download_file(data['file_hash'], data.get('resume', False))
            self.send_response(200)
            self.end_headers()
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
        self.http_server = HTTPServer(('0.0.0.0', self.peer_port), PeerHTTPHandler)
        self.http_server.peer = self
        threading.Thread(target=self.http_server.serve_forever, daemon=True).start()
        
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
        """Server lắng nghe kết nối từ peer khác - Phiên bản đa luồng"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('0.0.0.0', self.peer_port))
            s.listen(10)  # Tăng backlog connection
            print(f"[PEER SERVER] Listening on 0.0.0.0:{self.peer_port}")

            while self.is_running:
                try:
                    conn, addr = s.accept()
                    threading.Thread(
                        target=self._handle_peer_connection,
                        args=(conn, addr),
                        daemon=True
                    ).start()
                except Exception as e:
                    if self.is_running:
                        print(f"[PEER SERVER] Error: {str(e)}")

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
        """Phiên bản bền bỉ hơn với xử lý lỗi chi tiết"""
        max_retries = 3
        backoff_factor = 1
        
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    f"http://{self.tracker_host}:{self.tracker_port}",
                    json=data,
                    headers={'Content-Type': 'application/json'},
                    timeout=(3, 5))
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                error_type = type(e).__name__
                print(f"[TRACKER] Attempt {attempt+1} failed ({error_type}): {str(e)}")
                
                if attempt < max_retries - 1:
                    wait_time = backoff_factor * (attempt + 1)
                    print(f"[TRACKER] Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
        
        print("[TRACKER] All attempts failed")
        return None

    def _share_file(self, file_path):
        """Chia sẻ file lên tracker"""
        try:
            full_path = os.path.abspath(file_path)
            if not os.path.isfile(full_path):
                print(f"[-] File not found: {full_path}")
                return False

            print("[+] Calculating file hash...")
            file_hash = self._calculate_file_hash(full_path)
            print("[+] Splitting file into pieces...")
            pieces = self._split_file(full_path, file_hash)
            
            # Tạo metadata
            metadata = {
                "file_name": os.path.basename(full_path),
                "file_size": os.path.getsize(full_path),
                "piece_size": self.piece_size,
                "pieces": pieces,
                "file_hash": file_hash
            }
            
            # Gửi thông tin chia sẻ lên tracker
            response = self._send_to_tracker({
                "action": "share",
                "file_hash": file_hash,
                "file_name": os.path.basename(full_path),
                "pieces": pieces,
                "piece_availability": [1] * len(pieces), 
                "peer_id": self.peer_id,
                "port": self.peer_port,
                "ip": self._get_local_ip()
            })
            
            if response and response.get("status") == "success":
                print(f"[+] Shared successfully! File hash: {file_hash}")
                with self.lock:
                    self.shared_files[file_hash] = {
                        "file_name": os.path.basename(full_path),
                        "pieces": pieces,
                        "size": os.path.getsize(full_path)
                    }
                return True
            else:
                print("[-] Failed to share file with tracker")
                return False
        except Exception as e:
            print(f"[-] Share failed: {str(e)}")
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
        # Lấy hash từ tracker dựa trên tên file
        response = self._send_to_tracker({
            "action": "get_file_hash",
            "file_name": file_name
        })
        
        if not response or "file_hash" not in response:
            print(f"[-] File '{file_name}' not found on tracker")
            return False
        
        return self._download_file(response["file_hash"], resume)

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
        """Chọn peer tối ưu theo Rarest-First"""
        candidates = [p for p in peers if piece_idx in p.get("available_pieces", [])]
        
        if not candidates:
            return None
        
        # Ưu tiên peer có latency thấp và nhiều pieces hiếm
        def peer_score(peer):
            latency = peer.get("latency", 1.0)
            # Giả sử tracker cung cấp thông tin rare_pieces_count
            rare_count = peer.get("rare_pieces_count", 0)
            return (0.6 * (1 - min(latency, 1.0))) + (0.4 * rare_count)
        
        return max(candidates, key=peer_score)
    def _check_peer_connection(self, ip, port):
        """Kiểm tra kết nối đến peer trước khi download"""
        try:
            with socket.create_connection((ip, port), timeout=5) as s:
                s.sendall(b"PING\n")
                response = s.recv(1024)
                return response == b"PONG"
        except Exception as e:
            print(f"[CONNECTION] Failed to connect to {ip}:{port}: {str(e)}")
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
        """Phiên bản đã sửa lỗi biến success"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"[DOWNLOAD] Attempt {attempt+1} for piece {piece_idx} from {peer_ip}:{peer_port}")
                
                with socket.create_connection((peer_ip, peer_port), timeout=10) as s:
                    s.settimeout(30)
                    request = f"REQUEST {file_hash} {piece_idx}\n"
                    s.sendall(request.encode())
                    
                    piece_data = b""
                    while True:
                        chunk = s.recv(16384)
                        if not chunk:
                            break
                        piece_data += chunk
                    
                    if not piece_data:
                        print(f"[DOWNLOAD] Empty response for piece {piece_idx}")
                        continue
                        
                    if piece_data == b"PIECE_NOT_FOUND":
                        print(f"[DOWNLOAD] Piece {piece_idx} not found on peer")
                        return False
                    
                    # Lưu piece
                    piece_path = os.path.join(self.repository, f"{file_hash}_piece_{piece_idx}")
                    with open(piece_path, "wb") as f:
                        f.write(piece_data)
                    
                    print(f"[DOWNLOAD] Downloaded piece {piece_idx} ({len(piece_data)} bytes)")
                    
                    # Cập nhật tracker
                    self._send_to_tracker({
                        "action": "update_availability",
                        "file_hash": file_hash,
                        "piece_idx": piece_idx,
                        "peer_id": self.peer_id
                    })
                    return True  # Trả về trực tiếp nếu thành công
                    
            except Exception as e:
                print(f"[DOWNLOAD] Error for piece {piece_idx}: {str(e)}")
                time.sleep(1)
        
        print(f"[DOWNLOAD] Failed to download piece {piece_idx} after {max_retries} attempts")
        return False  # Trả về False nếu thất bại
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
        """Tải file với khả năng resume"""
        try:
            # Lấy metadata từ tracker
            metadata = self._get_metadata(file_hash)
            if not metadata:
                print("[-] Failed to get file metadata from tracker")
                return False

            # Khởi tạo thông tin download nếu chưa có
            with self.lock:
                if file_hash not in self.active_downloads:
                    self.active_downloads[file_hash] = {
                        "file_name": metadata["file_name"],
                        "total_pieces": len(metadata["pieces"]),
                        "downloaded": set(),
                        "status": "downloading"
                    }

            # Nếu resume, lấy các pieces đã có
            if resume:
                existing_pieces = self._get_existing_pieces(file_hash)
                with self.lock:
                    self.active_downloads[file_hash]["downloaded"].update(existing_pieces)
                print(f"[+] Resuming download, found {len(existing_pieces)} existing pieces")

            # Lấy danh sách peers từ tracker
            peers = self._get_peers(file_hash)
            if not peers:
                print("[-] No peers available for this file")
                return False

            print(f"[+] Found {len(peers)} peers with this file")

            # Xác định pieces cần tải
            needed_pieces = [
                p["index"] for p in metadata["pieces"] 
                if p["index"] not in self.active_downloads[file_hash]["downloaded"]
            ]

            if not needed_pieces:
                print("[+] All pieces already downloaded")
                return self._reconstruct_file(file_hash)

            print(f"[+] Need to download {len(needed_pieces)} pieces")

            # Tải song song với rarest-first strategy
            success = self._parallel_download(
                file_hash,
                needed_pieces,
                peers
            )

            if success and self._check_complete(file_hash):
                print("[+] All pieces downloaded, reconstructing file...")
                return self._reconstruct_file(file_hash)
            
            return False
            
        except Exception as e:
            print(f"[-] Download error: {str(e)}")
            return False
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
        """Ghép các mảnh thành file hoàn chỉnh với kiểm tra an toàn"""
        metadata_path = os.path.join(self.repository, f"{file_hash}_metadata.json")
        if not os.path.exists(metadata_path):
            print("[-] Metadata not found for reconstruction")
            return False

        with open(metadata_path, "r") as f:
            metadata = json.load(f)
        
        output_path = os.path.join(self.repository, metadata["file_name"])
        temp_path = output_path + ".temp"
        
        try:
            with open(temp_path, "wb") as out_file:
                for piece in metadata["pieces"]:
                    piece_path = os.path.join(self.repository, f"{file_hash}_piece_{piece['index']}")
                    if not os.path.exists(piece_path):
                        print(f"[-] Missing piece {piece['index']}, cannot reconstruct")
                        return False
                    
                    with open(piece_path, "rb") as piece_file:
                        out_file.write(piece_file.read())
            
            # Kiểm tra hash file hoàn chỉnh
            if self._calculate_file_hash(temp_path) == file_hash:
                os.replace(temp_path, output_path)
                print(f"[+] File reconstructed successfully: {output_path}")
                
                # Xóa các pieces và metadata đã tải
                self._cleanup_pieces(file_hash)
                return True
            else:
                print("[-] File integrity check failed")
                os.remove(temp_path)
                return False
                
        except Exception as e:
            print(f"[-] Error reconstructing file: {str(e)}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return False
    def _cleanup_pieces(self, file_hash):
        """Dọn dẹp các pieces sau khi ghép file thành công"""
        for f in os.listdir(self.repository):
            if f.startswith(f"{file_hash}_piece_") or f == f"{file_hash}_metadata.json":
                try:
                    os.remove(os.path.join(self.repository, f))
                except:
                    pass
        with self.lock:
            if file_hash in self.active_downloads:
                del self.active_downloads[file_hash]
            self._save_download_state()
    def _handle_discover(self):
        """Lấy danh sách file từ tracker"""
        response = self._send_to_tracker({"action": "discover"})
        if response and "files" in response:
            print("\nAvailable files:")
            for file in response["files"]:
                print(f"- {file['file_name']} (Hash: {file['hash'][:8]}...) [Peers: {file.get('active_peers', 0)}]")
        else:
            print("[-] Failed to discover files")

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
        peer = EnhancedPeer()
        peer.start()
    except KeyboardInterrupt:
        print("\n[!] Peer stopped by user")
    except Exception as e:
        print(f"[!] Fatal error: {str(e)}")