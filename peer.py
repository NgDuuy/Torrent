import socket
import threading
import json
import time
import hashlib
import os
import requests
from concurrent.futures import ThreadPoolExecutor

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

    def _download_file(self, file_hash):
        """Tải file từ các peer khác"""
        try:
            print(f"[+] Getting metadata for file {file_hash[:8]}...")
            # Lấy metadata trước
            metadata_response = self._send_to_tracker({
                "action": "get_metadata",
                "file_hash": file_hash
            })
            
            if not metadata_response or "metadata" not in metadata_response:
                print("[-] Failed to get file metadata")
                return False

            metadata = metadata_response["metadata"]
            # Lưu metadata trước khi download
            with open(os.path.join(self.repository, f"{file_hash}_metadata.json"), "w") as f:
                json.dump(metadata, f)

            print(f"[+] Getting peers for file {file_hash[:8]}...")
            peers_response = self._send_to_tracker({
                "action": "get_peers", 
                "file_hash": file_hash
            })
            
            if not peers_response or "peers" not in peers_response:
                print("[-] No peers available for this file")
                return False

            peers = peers_response["peers"]
            print(f"[+] Found {len(peers)} peers with this file")
            
            with self.lock:
                self.active_downloads[file_hash] = {
                    "total_pieces": len(metadata["pieces"]),
                    "downloaded": set()
                }

            # Tải từng mảnh
            for piece in metadata["pieces"]:
                piece_idx = piece["index"]
                for peer in peers:
                    if piece_idx in peer.get("available_pieces", []):
                        if self._download_piece(file_hash, piece_idx, peer["ip"], peer["port"]):
                            break
                else:
                    print(f"[-] Failed to download piece {piece_idx}")
                    return False

            # Ghép file
            if self._check_complete(file_hash):
                self._reconstruct_file(file_hash)
                print(f"[+] Successfully downloaded file: {metadata['file_name']}")
                return True
            
            print("[-] Failed to download all pieces")
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
        """Ghép các mảnh thành file hoàn chỉnh"""
        metadata_path = os.path.join(self.repository, f"{file_hash}_metadata.json")
        if not os.path.exists(metadata_path):
            print("[-] Metadata not found for reconstruction")
            return False

        with open(metadata_path, "r") as f:
            metadata = json.load(f)
        
        output_path = os.path.join(self.repository, metadata["file_name"])
        try:
            with open(output_path, "wb") as out_file:
                for piece in metadata["pieces"]:
                    piece_path = os.path.join(self.repository, f"{file_hash}_piece_{piece['index']}")
                    with open(piece_path, "rb") as piece_file:
                        out_file.write(piece_file.read())
            
            print(f"[+] Reconstructed file saved to: {output_path}")
            return True
        except Exception as e:
            print(f"[-] Error reconstructing file: {str(e)}")
            return False

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