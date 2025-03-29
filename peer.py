import socket
import threading
import json
import time
import hashlib
import os
import random
import requests
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, HTTPServer
class PeerHTTPHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data)
        
        if self.path == '/download':
            # Xử lý lệnh download
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
        self.executor = ThreadPoolExecutor(max_workers=5)
        self._load_download_state()  # Tải trạng thái khi khởi động
        self.http_server = HTTPServer(('0.0.0.0', self.peer_port), PeerHTTPHandler)
        self.http_server.peer = self
        threading.Thread(target=self.http_server.serve_forever, daemon=True).start()
        os.makedirs(self.repository, exist_ok=True)
        print(f"Peer ID: {self.peer_id}")
    def start(self):
        server_thread = threading.Thread(target=self._run_server, daemon=True)
        server_thread.start()
        print(f"\nPeer server running on port {self.peer_port}")
        print("Commands: discover | share <file> | download <hash> | list | status | exit\n")
        self._command_interface()

    def _run_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(('0.0.0.0', self.peer_port))
                s.listen(5)
                while True:
                    conn, addr = s.accept()
                    self.executor.submit(self._handle_peer_connection, conn, addr)
            except Exception as e:
                print(f"[!] Peer server error: {str(e)}")

    def _handle_peer_connection(self, conn, addr):
        """Xử lý yêu cầu từ peer khác"""
        try:
            data = conn.recv(1024).decode()
            if data.startswith("REQUEST"):
                _, file_hash, piece_idx = data.split()
                self._send_piece(conn, file_hash, int(piece_idx))
        except Exception as e:
            print(f"[-] Error handling peer {addr}: {str(e)}")
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
        """Giao diện dòng lệnh tương tác với người dùng"""
        while True:
            try:
                cmd = input("Peer> ").strip().split()
                if not cmd:
                    continue
                
                if cmd[0] == "discover":
                    self._handle_discover()
                elif cmd[0] == "share" and len(cmd) > 1:
                    self._share_file(cmd[1])
                elif cmd[0] == "download" and len(cmd) > 1:
                    if len(cmd) > 2 and cmd[2] == "--resume":
                        self._download_file(cmd[1], resume=True)
                    else:
                        self._download_file(cmd[1])
                elif cmd[0] == "list":
                    self._list_shared_files()
                elif cmd[0] == "status":
                    self._check_download_status()
                elif cmd[0] == "exit":
                    self._graceful_exit()
                    break
                else:
                    print("Unknown command. Available commands:")
                    print("discover | share <file> | download <hash> | list | status | exit")
            except Exception as e:
                print(f"Command error: {str(e)}")

    def _send_to_tracker(self, data):
        max_retries = 3
        backoff_factor = 1  # Thời gian chờ tăng dần
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    f"http://{self.tracker_host}:{self.tracker_port}",
                    json=data,
                    headers={'Content-Type': 'application/json'},
                    timeout=(3, 5)  # Connect timeout 3s, read timeout 5s
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                wait_time = backoff_factor * (attempt + 1)
                print(f"[!] Attempt {attempt+1} failed ({type(e).__name__}), retrying in {wait_time}s...")
                time.sleep(wait_time)
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
            response = self._send_to_tracker({
                "action": "share",
                "file_hash": file_hash,
                "metadata": metadata,
                "file_name": os.path.basename(full_path),
                "pieces": pieces,
                "peer_id": self.peer_id,
                "port": self.peer_port,
                "ip": self._get_local_ip()
            })
            
            if response and response.get("status") == "success":
                print(f"[+] Shared successfully! File hash: {file_hash}")
                with self.lock:
                    self.shared_files[file_hash] = os.path.basename(full_path)
                return True
            else:
                print("[-] Failed to share file")
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

    def _parallel_download(self, file_hash, needed_pieces, peers, max_workers=5):
        """Tải song song với multi-source"""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for piece_idx in needed_pieces:
                # Chọn peer tối ưu cho mỗi piece
                peer = self._select_peer_for_piece(piece_idx, peers)
                if not peer:
                    continue
                    
                futures.append(
                    executor.submit(
                        self._download_piece_optimized,
                        file_hash,
                        piece_idx,
                        peer["ip"],
                        peer["port"]
                    )
                )
            
            # Theo dõi tiến trình
            success = True
            for future in as_completed(futures):
                if not future.result():
                    success = False
                    executor.shutdown(wait=False)
                    break
                    
            return success

    def _select_peer_for_piece(self, piece_idx, peers):
        """Chọn peer có piece với latency thấp nhất"""
        candidates = [
            p for p in peers 
            if piece_idx in p.get("available_pieces", [])
        ]
        return min(candidates, key=lambda x: x["latency"], default=None)
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
    def _download_piece_optimized(self, file_hash, piece_idx, peer_ip, peer_port):
        """Phiên bản tối ưu với timeout và đo tốc độ"""
        try:
            start_time = time.time()
            with socket.create_connection((peer_ip, peer_port), timeout=5) as s:
                s.settimeout(10)  # Timeout cho mỗi piece
                s.sendall(f"REQUEST {file_hash} {piece_idx}".encode())
                
                # Nhận dữ liệu theo chunk
                piece_data = b""
                while True:
                    chunk = s.recv(16384)  # 16KB/chunk
                    if not chunk:
                        break
                    piece_data += chunk

                if piece_data == b"PIECE_NOT_FOUND":
                    return False
                    
                # Lưu piece
                piece_path = os.path.join(self.repository, f"{file_hash}_piece_{piece_idx}")
                with open(piece_path, "wb") as f:
                    f.write(piece_data)
                
                # Log tốc độ
                duration = max(time.time() - start_time, 0.001)
                speed = len(piece_data) / duration / 1024  # KB/s
                print(f"[✓] Piece {piece_idx} from {peer_ip}:{peer_port} | Speed: {speed:.2f} KB/s")
                
                # Cập nhật trạng thái
                with self.lock:
                    if file_hash not in self.active_downloads:
                        self.active_downloads[file_hash] = {"downloaded": set()}
                    self.active_downloads[file_hash]["downloaded"].add(piece_idx)
                
                return True
                
        except Exception as e:
            print(f"[×] Piece {piece_idx} error: {str(e)}")
            return False
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
            # Lấy metadata
            metadata = self._get_metadata(file_hash)
            if not metadata:
                print("[-] Failed to get file metadata")
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

            # Lấy danh sách peers
            peers = self._get_peers(file_hash)
            if not peers:
                print("[-] No peers available")
                return False

            # Xác định pieces cần tải
            needed_pieces = [
                p["index"] for p in metadata["pieces"] 
                if p["index"] not in self.active_downloads[file_hash]["downloaded"]
            ]

            if not needed_pieces:
                print("[+] All pieces already downloaded")
                return self._reconstruct_file(file_hash)

            print(f"[+] Downloading {len(needed_pieces)} missing pieces from {len(peers)} peers...")
            
            # Tải song song
            success = self._parallel_download(
                file_hash,
                needed_pieces,
                peers,
                max_workers=5
            )

            # Ghép file nếu thành công
            if success and self._check_complete(file_hash):
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
        """Liệt kê các file đang chia sẻ"""
        if not self.shared_files:
            print("No files are being shared")
            return
        
        print("\nShared files:")
        for file_hash, file_name in self.shared_files.items():
            print(f"- {file_name} (Hash: {file_hash[:8]}...)")

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
        self._save_download_state()  # Lưu trạng thái trước khi thoát
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